import re
from collections import defaultdict
from io import StringIO
from itertools import groupby
from operator import itemgetter
from typing import Union, Optional, Tuple, List, TextIO, BinaryIO, Set, Dict

import pandas as pd
import numpy as np
from mavehgvs import MaveHgvsParseError, Variant
from fqfa.util.translate import translate_dna
from fqfa.util.infer import infer_sequence_type

from . import constants
from .utils import is_csv_null, NULL_VALUES, READABLE_NULL_VALUES
from ..validation.constants.general import null_values_re


class MaveDataset:
    class DatasetType:
        SCORES = "scores"
        COUNTS = "counts"

    class HGVSColumns:
        NUCLEOTIDE: str = constants.HGVS_NT_COLUMN
        TRANSCRIPT: str = constants.HGVS_SPLICE_COLUMN
        PROTEIN: str = constants.HGVS_PRO_COLUMN

        @classmethod
        def options(cls) -> List[str]:
            return [cls.NUCLEOTIDE, cls.TRANSCRIPT, cls.PROTEIN]

    class AdditionalColumns:
        @classmethod
        def options(cls) -> List[str]:
            return []

    # ---------------------- Construction------------------------------------ #
    @classmethod
    def for_scores(cls, file: Union[str, TextIO, BinaryIO]) -> "MaveScoresDataset":
        return cls._for_type(file=file, dataset_type=cls.DatasetType.SCORES)

    @classmethod
    def for_counts(cls, file: Union[str, TextIO, BinaryIO]) -> "MaveCountsDataset":
        return cls._for_type(file=file, dataset_type=cls.DatasetType.COUNTS)

    @classmethod
    def _for_type(
        cls, file: Union[str, TextIO, BinaryIO], dataset_type: str
    ) -> Union["MaveScoresDataset", "MaveCountsDataset"]:

        if isinstance(file, str):
            handle = file
        elif hasattr(file, "read"):
            file_contents = file.read()
            if hasattr(file_contents, "decode"):
                file_contents = file_contents.decode("utf-8")
            file_contents = file_contents.strip()
            handle = StringIO(file_contents)
        else:
            raise TypeError(f"Expected file path or buffer object. " f"Got '{type(file).__name__}'")

        extra_na_values = set(
            list(NULL_VALUES)
            + [str(x).lower() for x in NULL_VALUES]
            + [str(x).upper() for x in NULL_VALUES]
            + [str(x).capitalize() for x in NULL_VALUES]
        )

        df = pd.read_csv(
            filepath_or_buffer=handle,
            sep=",",
            encoding="utf-8",
            quotechar='"',
            comment="#",
            na_values=extra_na_values,
            keep_default_na=True,
            dtype={
                **{c: str for c in cls.HGVSColumns.options()},
                MaveScoresDataset.AdditionalColumns.SCORES: float,
            },
        ).replace(null_values_re, np.NaN)

        if dataset_type == cls.DatasetType.SCORES:
            return MaveScoresDataset(df)
        elif dataset_type == cls.DatasetType.COUNTS:
            return MaveCountsDataset(df)
        else:
            raise ValueError(f"'{dataset_type}' is not a recognised dataset type.")

    # ---------------------- Public ----------------------------------------- #
    @property
    def label(self) -> str:
        return "dataset"

    @property
    def is_valid(self) -> Optional[bool]:
        if self._errors is None:
            return None
        return len(self._errors) == 0

    @property
    def n_errors(self) -> Optional[int]:
        if self._errors is None:
            return None
        return len(self._errors)

    @property
    def errors(self) -> Optional[List[str]]:
        return self._errors

    @property
    def is_empty(self) -> bool:
        return self._df.empty

    @property
    def columns(self) -> List[str]:
        return list(self._df.columns)

    @property
    def hgvs_columns(self) -> List[str]:
        return [c for c in self.columns if c in self.HGVSColumns.options()]

    @property
    def non_hgvs_columns(self) -> List[str]:
        return [c for c in self.columns if c not in self.HGVSColumns.options()]

    @property
    def n_rows(self) -> int:
        return len(self._df)

    @property
    def n_columns(self) -> int:
        return len(self.columns)

    @property
    def index_column(self) -> Optional[str]:
        if self._errors:
            return None
        return self._index_column

    @property
    def index(self) -> Optional[pd.Index]:
        if self._errors:
            return None
        return self._df.index.copy(deep=True)

    def data(self, serializable=False) -> pd.DataFrame:
        """
        Return underlying dataframe object.

        Parameters
        ----------
        serializable: bool
            Replaces `np.NaN` with `None` for JSON compatibility.
        """
        if serializable:
            return self._df.where(cond=pd.notnull(self._df), other=None, inplace=False)
        return self._df.copy(deep=True)

    def match_other(self, other: "MaveDataset") -> Optional[bool]:
        """
        Check that each dataset defined the same variants in each column.

        Parameters
        ----------
        other: MaveDataset
            Validator instance to match against.

        Returns
        -------
        A boolean indicating index match, otherwise `None` if either instance
        is not valid.
        """
        if (not self.is_valid) or (not other.is_valid):
            return None

        if self.index_column != other.index_column:
            return False

        return all(self._df[column].equals(other._df[column]) for column in self.HGVSColumns.options())

    def to_dict(self) -> Dict[str, Dict]:
        """
        Returns underlying dataframe as dictionary in 'records' orientation.
        Keys will be index values and values will be an inner dictionary mapping
        column names to row values for said index.
        """
        # Convert np.NaN values to None for consistency across all columns and
        # for compatibility in PostgresSQL queries. Replaces all values which
        # are considered null by pandas with None by masking pd.notnull cells.
        return self._df.where(cond=pd.notnull(self._df), other=None, inplace=False).to_dict(orient="index")

    def validate(
        self,
        targetseq: Optional[str] = None,
        relaxed_ordering: bool = False,
        allow_index_duplicates: bool = False,
    ) -> "MaveDataset":

        self._errors = []
        self._df.index = pd.RangeIndex(start=0, stop=self.n_rows, step=1)
        self._index_column = None

        self._validate_columns()
        # Only attempt to validate variants if columns are valid
        if not self._errors:
            (
                self._normalize_data()
                ._validate_genomic_variants(targetseq, relaxed_ordering)
                ._validate_transcript_variants(targetseq, relaxed_ordering)
                ._validate_protein_variants(targetseq, relaxed_ordering)
                ._validate_index_column(allow_duplicates=allow_index_duplicates)
            )

        if self.is_empty:
            self._errors.append(
                f"No variants could be parsed from your {self.label} file. " f"Please upload a non-empty file."
            )
            return self

        if not self._errors:
            # Set index last as original index is used when indicating duplicate
            # hgvs string row numbers in the column name used as the index (
            # either hgvs_nt when present or hgvs_pro when hgvs_nt is absent).
            self._df.index = pd.Index(self._df[self.index_column])

        return self

    # ---------------------- Private ---------------------------------------- #
    def __init__(
        self,
        df: Optional[pd.DataFrame] = None,
        index_column: Optional[str] = None,
        errors: Optional[List[str]] = None,
    ):
        self._df: pd.DataFrame = pd.DataFrame() if df is None else df
        self._index_column = index_column or None
        self._errors = None if errors is None else list(errors)

    def __repr__(self):
        return (
            f"<"
            f"{self.__class__.__name__} "
            f"columns={self.columns} "
            f"index={self.index_column} "
            f"valid={self.is_valid}"
            f">"
        )

    @property
    def _column_order(self) -> Dict[str, int]:
        return defaultdict(
            lambda: 100,
            {
                self.HGVSColumns.NUCLEOTIDE: 0,
                self.HGVSColumns.TRANSCRIPT: 1,
                self.HGVSColumns.PROTEIN: 2,
                **{c: (2 + i) for (i, c) in enumerate(self.AdditionalColumns.options(), start=1)},
            },
        )

    def _validate_columns(self) -> "MaveDataset":
        if self._errors:
            return self

        # Pandas will automatically name blank columns using the pattern below
        unnamed = re.compile(r"^Unnamed: \d+$", flags=re.IGNORECASE)
        columns = self.columns
        if any(is_csv_null(h) or unnamed.match(h) for h in columns):
            self._errors.append(
                f"Column names in your {self.label} file cannot values "
                f"considered null such as the following: "
                f"{', '.join(READABLE_NULL_VALUES)}"
            )

        columns = [c for c in columns if not is_csv_null(c)]
        if len(columns) < 1:
            self._errors.append(
                f"No columns could not be parsed from your {self.label} file. "
                "Make sure columns are comma delimited. Column names with "
                "commas must be escaped by enclosing them in double quotes"
            )

        required = {
            self.HGVSColumns.NUCLEOTIDE,
            self.HGVSColumns.PROTEIN,
        }
        if not (set(columns) & required):
            self._errors.append(
                f"Your {self.label} file must define either a nucleotide "
                f"hgvs column '({self.HGVSColumns.NUCLEOTIDE})' "
                f"or a protein hgvs column '({self.HGVSColumns.PROTEIN})'. "
                f"Columns are case-sensitive and must be comma delimited"
            )

        if not (set(columns) - set(self.HGVSColumns.options())):
            self._errors.append(
                f"Your {self.label} file must define at least one additional "
                f"column different from '{self.HGVSColumns.NUCLEOTIDE}', "
                f"'{self.HGVSColumns.TRANSCRIPT}' and "
                f"'{self.HGVSColumns.PROTEIN}'"
            )

        return self

    def _normalize_data(self) -> "MaveDataset":
        if self._errors:
            return self

        # Initialize missing hgvs columns as empty.
        for c in self.HGVSColumns.options():
            if c not in self.columns:
                self._df[c] = np.NaN

        column_order = self._column_order
        sorted_columns = list(sorted(self.columns, key=lambda x: column_order[x]))

        self._df = self._df[sorted_columns]
        return self

    def _validate_genomic_variants(
        self, targetseq: Optional[str] = None, relaxed_ordering: bool = False
    ) -> "MaveDataset":
        if self._column_is_null(self.HGVSColumns.NUCLEOTIDE):
            return self

        defines_transcript_variants = not self._column_is_null(self.HGVSColumns.TRANSCRIPT)
        validated_variants, prefixes, errors = self._validate_variants(
            column=self.HGVSColumns.NUCLEOTIDE,
            splice_defined=defines_transcript_variants,
            targetseq=targetseq,
            relaxed_ordering=relaxed_ordering,
        )

        if ("c" in prefixes or "n" in prefixes) and "g" in prefixes:
            self._errors.append(
                f"{self.HGVSColumns.NUCLEOTIDE}: Genomic variants "
                f"(prefix 'g.') cannot be mixed with transcript variants "
                f"(prefix 'c.' or 'n.')"
            )

        if prefixes == {"g"} and not defines_transcript_variants:
            self._errors.append(
                f"Transcript variants ('{self.HGVSColumns.TRANSCRIPT}' column) "
                f"are required when specifying genomic variants "
                f"(prefix 'g.' in the 'hgvs_nt' column)"
            )

        self._errors += errors

        if not self._errors:
            self._df[self.HGVSColumns.NUCLEOTIDE] = validated_variants

        self._index_column = self.HGVSColumns.NUCLEOTIDE
        return self

    def _validate_transcript_variants(
        self, targetseq: Optional[str] = None, relaxed_ordering: bool = False
    ) -> "MaveDataset":
        defines_nt = not self._column_is_null(self.HGVSColumns.NUCLEOTIDE)
        defines_tx = not self._column_is_null(self.HGVSColumns.TRANSCRIPT)

        if defines_tx and (not defines_nt):
            self._errors.append(
                f"Genomic variants ('{self.HGVSColumns.NUCLEOTIDE}' column) "
                f"must be defined when specifying transcript "
                f"variants ('{self.HGVSColumns.TRANSCRIPT}' column)"
            )

        if not defines_tx:
            return self

        # Don't validate transcript variants against sequence. Might come
        # back to this later with research into implementing gene models.
        validated_variants, _, errors = self._validate_variants(
            column=self.HGVSColumns.TRANSCRIPT,
            targetseq=None,
            relaxed_ordering=relaxed_ordering,
        )

        self._errors += errors

        if not self._errors:
            self._df[self.HGVSColumns.TRANSCRIPT] = validated_variants

        return self

    def _validate_protein_variants(
        self, targetseq: Optional[str] = None, relaxed_ordering: bool = False
    ) -> "MaveDataset":
        if self._column_is_null(self.HGVSColumns.PROTEIN):
            return self

        defines_nt = not self._column_is_null(self.HGVSColumns.NUCLEOTIDE)
        defines_splice = not self._column_is_null(self.HGVSColumns.TRANSCRIPT)

        if defines_splice:
            protein_seq = None
        else:
            protein_seq = targetseq
            if targetseq and "dna" in infer_sequence_type(targetseq).lower():
                protein_seq, remainder = translate_dna(targetseq)
                if remainder:
                    self._errors.insert(
                        0,
                        "Protein variants could not be validated because the "
                        "length of your target sequence is not a multiple of 3",
                    )

        validated_variants, _, errors = self._validate_variants(
            column=self.HGVSColumns.PROTEIN,
            targetseq=protein_seq,
            relaxed_ordering=relaxed_ordering,
        )

        self._errors += errors

        if not self._errors:
            self._df[self.HGVSColumns.PROTEIN] = validated_variants

        if not defines_nt:
            self._index_column = self.HGVSColumns.PROTEIN

        return self

    def _validate_index_column(self, allow_duplicates: bool = False) -> "MaveDataset":
        if self._errors:
            return self

        if self._index_column is None:
            self._index_column = self.HGVSColumns.NUCLEOTIDE

        if self._column_is_partially_null(self._index_column):
            self._errors.append(
                f"Primary column (inferred as '{self._index_column}') "
                f"cannot contain any null values from "
                f"{', '.join(READABLE_NULL_VALUES)} (case-insensitive)"
            )

        if not allow_duplicates:
            dupes = self._df[self._index_column].duplicated(keep=False)
            if np.any(dupes):
                dup_list = zip(
                    self._df.loc[dupes, self._index_column],
                    dupes.index[dupes],
                )
                dupes_str = ", ".join(
                    f"{v}: {[(g[1] + 1) for g in groups]}"  # get row numbers
                    for (v, groups) in groupby(dup_list, key=itemgetter(0))
                )
                self._errors.append(
                    f"Primary column (inferred as '{self._index_column}') "
                    f"contains duplicate HGVS variants: {dupes_str}"
                )

        return self

    def _validate_variants(
        self,
        column: str,
        splice_defined: Optional[bool] = None,
        targetseq: Optional[str] = None,
        relaxed_ordering: bool = False,
    ) -> Tuple[pd.Series, Set[str], List[str]]:

        prefixes = set()
        errors = []

        def validate_variant(variant: str):
            # TODO: logic mirrors that in validate_hgvs_string, which is kept
            #   as a standalone function for backwards compatibility with
            #   django's model validator field. Merge at some point.

            if is_csv_null(variant):
                return np.NaN
            else:
                try:
                    if variant.lower() == "_sy":
                        errors.append("'_sy' is no longer supported and should be " "replaced by 'p.(=)'")
                        return variant
                    elif variant.lower() == "_wt":
                        errors.append(
                            "'_wt' is no longer supported and should be " "replaced by one of 'g.=', 'c.=' or 'n.='"
                        )
                        return variant

                    validated = Variant(
                        variant,
                        targetseq=targetseq,
                        relaxed_ordering=relaxed_ordering,
                    )
                    prefix = validated.prefix.lower()
                    prefixes.add(prefix)

                    prefix_error = self._validate_variant_prefix_for_column(
                        variant=validated,
                        prefix=validated.prefix,
                        column=column,
                        splice_defined=splice_defined,
                    )
                    if prefix_error:
                        errors.append(prefix_error)

                    return str(validated)

                except MaveHgvsParseError as error:
                    errors.append(f"{variant}: {str(error)}")
                    return np.NaN

        validated_variants = self._df[column].apply(validate_variant)

        return validated_variants, prefixes, errors

    def _column_is_null(self, column) -> bool:
        return len(self._df[self._df[column].isna()]) == len(self._df)

    def _column_is_partially_null(self, column) -> bool:
        return 0 < len(self._df[self._df[column].isna()]) < len(self._df)

    def _column_is_fully_specified(self, column) -> bool:
        return len(self._df[self._df[column].isna()]) == 0

    def _validate_variant_prefix_for_column(
        self, variant: Variant, prefix: str, column: str, splice_defined: bool
    ) -> Optional[str]:
        prefix = prefix.lower()

        if column == self.HGVSColumns.NUCLEOTIDE:
            if splice_defined:
                if prefix not in "g":
                    return (
                        f"{column}: "
                        f"'{variant}' is not a genomic variant "
                        f"(prefix 'g.'). Nucleotide variants must "
                        f"be genomic if transcript variants are "
                        f"also present"
                    )
            else:
                if prefix not in "cn":
                    return (
                        f"{column}: "
                        f"'{variant}' is not a transcript variant. "
                        f"The accepted transcript variant prefixes "
                        f"are 'c.' or 'n.'"
                    )
        elif column == self.HGVSColumns.TRANSCRIPT:
            if prefix not in "cn":
                return (
                    f"{column}: "
                    f"'{variant}' is not a transcript variant. The "
                    f"accepted transcript variant prefixes are "
                    f"'c.' or 'n.'"
                )
        elif column == self.HGVSColumns.PROTEIN:
            if prefix not in "p":
                return (
                    f"{column}: "
                    f"'{variant}' is not a protein variant. "
                    f"The accepted protein variant prefix is 'p.'"
                )
        else:
            raise ValueError(f"Unknown column '{column}'. Expected one " f"of {', '.join(self.HGVSColumns.options())}")

        return None


class MaveScoresDataset(MaveDataset):
    class AdditionalColumns:
        SCORES = constants.REQUIRED_SCORE_COLUMN

        @classmethod
        def options(cls) -> List[str]:
            return [cls.SCORES]

    @property
    def label(self) -> str:
        return "scores"

    def _validate_columns(self) -> "MaveDataset":
        super()._validate_columns()

        if self.AdditionalColumns.SCORES not in self.columns:
            self._errors.append(
                f"Your scores dataset is missing the "
                f"'{self.AdditionalColumns.SCORES}' column. "
                f"Columns are case-sensitive and must be comma delimited"
            )

        return self

    def _normalize_data(self) -> "MaveDataset":
        super()._normalize_data()

        should_be_numeric = [self.AdditionalColumns.SCORES]
        for c in should_be_numeric:
            if c in self.columns:
                try:
                    self._df[c] = self._df[c].astype(dtype=float, errors="raise")
                except ValueError as e:
                    self._errors.append(f"{c}: {str(e)}")

        return self


class MaveCountsDataset(MaveDataset):
    @property
    def label(self) -> str:
        return "counts"
