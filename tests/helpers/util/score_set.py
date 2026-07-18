from copy import deepcopy
from datetime import date
from typing import Any, Dict, Optional
from unittest.mock import patch

import cdot.hgvs.dataproviders
import jsonschema
from fastapi.testclient import TestClient
from sqlalchemy import select

from mavedb.models.clinical_control import ClinvarControl as ClinicalControlDbModel
from mavedb.models.gnomad_variant import GnomADVariant as GnomADVariantDbModel
from mavedb.models.mapped_variant import MappedVariant as MappedVariantDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.view_models.score_set import ScoreSet, ScoreSetCreate
from tests.helpers.constants import (
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_MULTI_TARGET_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_NT_CDOT_TRANSCRIPT,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK,
    TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X,
    TEST_VALID_PRE_MAPPED_VRS_CIS_PHASED_BLOCK,
)
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record
from tests.helpers.util.variant import mock_worker_variant_insertion


def create_seq_score_set(
    client: TestClient, experiment_urn: Optional[str], update: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    score_set_payload = deepcopy(TEST_MINIMAL_SEQ_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.model_json_schema())

    response = client.post("/api/v1/score-sets/", json=score_set_payload)
    assert response.status_code == 200, "Could not create sequence based score set"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())
    return response_data


def create_acc_score_set(
    client: TestClient, experiment_urn: Optional[str], update: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    score_set_payload = deepcopy(TEST_MINIMAL_ACC_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)

    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.model_json_schema())

    with patch.object(
        cdot.hgvs.dataproviders.RESTDataProvider, "_get_transcript", return_value=TEST_NT_CDOT_TRANSCRIPT
    ):
        response = client.post("/api/v1/score-sets/", json=score_set_payload)

    assert response.status_code == 200, "Could not create accession based score set"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())
    return response_data


def create_multi_target_score_set(
    client: TestClient, experiment_urn: Optional[str], update: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    score_set_payload = deepcopy(TEST_MINIMAL_MULTI_TARGET_SCORESET)
    if experiment_urn is not None:
        score_set_payload["experimentUrn"] = experiment_urn
    if update is not None:
        score_set_payload.update(update)
    jsonschema.validate(instance=score_set_payload, schema=ScoreSetCreate.model_json_schema())

    response = client.post("/api/v1/score-sets/", json=score_set_payload)
    assert response.status_code == 200, "Could not create sequence based score set"

    response_data = response.json()
    jsonschema.validate(instance=response_data, schema=ScoreSet.model_json_schema())
    return response_data


def create_seq_score_set_with_mapped_variants(
    client,
    db,
    data_provider,
    experiment_urn,
    scores_csv_path,
    update=None,
    counts_csv_path=None,
    score_columns_metadata_json_path=None,
    count_columns_metadata_json_path=None,
):
    score_set = create_seq_score_set_with_variants(
        client,
        db,
        data_provider,
        experiment_urn,
        scores_csv_path,
        update,
        counts_csv_path,
        score_columns_metadata_json_path,
        count_columns_metadata_json_path,
    )
    score_set = mock_worker_vrs_mapping(client, db, score_set)

    jsonschema.validate(instance=score_set, schema=ScoreSet.model_json_schema())
    return score_set


def create_acc_score_set_with_mapped_variants(
    client,
    db,
    data_provider,
    experiment_urn,
    scores_csv_path,
    update=None,
    counts_csv_path=None,
    score_columns_metadata_json_path=None,
    count_columns_metadata_json_path=None,
):
    score_set = create_acc_score_set_with_variants(
        client,
        db,
        data_provider,
        experiment_urn,
        scores_csv_path,
        update,
        counts_csv_path,
        score_columns_metadata_json_path,
        count_columns_metadata_json_path,
    )
    score_set = mock_worker_vrs_mapping(client, db, score_set)

    jsonschema.validate(instance=score_set, schema=ScoreSet.model_json_schema())
    return score_set


def create_seq_score_set_with_variants(
    client,
    db,
    data_provider,
    experiment_urn,
    scores_csv_path,
    update=None,
    counts_csv_path=None,
    score_columns_metadata_json_path=None,
    count_columns_metadata_json_path=None,
):
    score_set = create_seq_score_set(client, experiment_urn, update)
    score_set = mock_worker_variant_insertion(
        client,
        db,
        data_provider,
        score_set,
        scores_csv_path,
        counts_csv_path,
        score_columns_metadata_json_path,
        count_columns_metadata_json_path,
    )

    assert (
        score_set["numVariants"] == 3
    ), f"Could not create sequence based score set with variants within experiment {experiment_urn}"

    jsonschema.validate(instance=score_set, schema=ScoreSet.model_json_schema())
    return score_set


def create_acc_score_set_with_variants(
    client,
    db,
    data_provider,
    experiment_urn,
    scores_csv_path,
    update=None,
    counts_csv_path=None,
    score_columns_metadata_json_path=None,
    count_columns_metadata_json_path=None,
):
    score_set = create_acc_score_set(client, experiment_urn, update)
    score_set = mock_worker_variant_insertion(
        client,
        db,
        data_provider,
        score_set,
        scores_csv_path,
        counts_csv_path,
        score_columns_metadata_json_path,
        count_columns_metadata_json_path,
    )

    assert (
        score_set["numVariants"] == 3
    ), f"Could not create sequence based score set with variants within experiment {experiment_urn}"

    jsonschema.validate(instance=score_set, schema=ScoreSet.model_json_schema())
    return score_set


def seed_annotation_substrate(db, score_set, *, skip_first=False):
    """Give a score set's variants a live ``MappingRecord`` with one authoritative allele.

    The VA endpoints build a ``VariantAnnotationContext`` from the ``MappingRecord`` / ``Allele``
    substrate (not the legacy ``MappedVariant``), so a variant needs a live record with an
    authoritative ``post_mapped`` allele before a study result / statement can be constructed. This
    mirrors what the mapping pipeline writes. (Global seeding from ``mock_worker_vrs_mapping`` is
    deferred to Slice 5.2/5.3, where it can be reconciled with the clinical-controls seeding.)

    With ``skip_first=True`` the first variant is left un-mapped on the new substrate (for the
    "some variants were not mapped" cases). Returns the score set's variants in id order.
    """
    variants = db.scalars(
        select(VariantDbModel)
        .join(ScoreSetDbModel)
        .where(ScoreSetDbModel.urn == score_set["urn"])
        .order_by(VariantDbModel.id)
    ).all()
    for variant in variants[1:] if skip_first else variants:
        seed_mapping_record(
            db,
            variant,
            alleles=[
                AlleleSpec(
                    digest=f"va-allele-{variant.id}",
                    is_authoritative=True,
                    clingen_allele_id=f"CA{variant.id}",
                    post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
                )
            ],
        )
    return variants


def link_clinical_controls_to_alleles(db, score_set):
    """Seed the new-model annotation graph for a score set's first two variants and link the two
    seeded ClinVar controls to their alleles via ``ClinvarAlleleLink``.

    The first variant's allele gets the ClinVar control (id 1), the second gets the generic control
    (id 2) — mirroring the two-control shape the clinical-controls routes are asserted against.
    """
    variants = db.scalars(
        select(VariantDbModel)
        .join(ScoreSetDbModel)
        .where(ScoreSetDbModel.urn == score_set["urn"])
        .order_by(VariantDbModel.id)
    ).all()

    seed_mapping_record(
        db,
        variants[0],
        alleles=[AlleleSpec(digest="clinical-control-allele-0", is_authoritative=True, clinvar_control_ids=[1])],
    )
    seed_mapping_record(
        db,
        variants[1],
        alleles=[AlleleSpec(digest="clinical-control-allele-1", is_authoritative=True, clinvar_control_ids=[2])],
    )


def link_clinvar_control_to_mapped_variant(db, score_set):
    """Link the seeded ClinVar clinical control (id=1) to the first mapped variant of a score set."""
    mapped_variants = db.scalars(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .where(ScoreSetDbModel.urn == score_set["urn"])
    ).all()

    mapped_variants[0].clinical_controls.append(
        db.scalar(select(ClinicalControlDbModel).where(ClinicalControlDbModel.id == 1))
    )

    db.add(mapped_variants[0])
    db.commit()


def link_gnomad_variants_to_mapped_variants(db, score_set):
    mapped_variants = db.scalars(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .where(ScoreSetDbModel.urn == score_set["urn"])
    ).all()

    # The first mapped variant gets the gnomAD variant.
    mapped_variants[0].gnomad_variants.append(
        db.scalar(select(GnomADVariantDbModel).where(GnomADVariantDbModel.id == 1))
    )

    db.add(mapped_variants[0])
    db.add(mapped_variants[1])
    db.commit()


def mock_worker_vrs_mapping(client, db, score_set, alleles=True):
    # The mapping job is tested elsewhere, so insert mapped variants manually.
    variants = db.scalars(
        select(VariantDbModel).join(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set["urn"])
    ).all()

    # It's un-important what the contents of each mapped VRS object are, so use the same constant for each variant.
    for variant in variants:
        mapped_variant = MappedVariantDbModel(
            pre_mapped=TEST_VALID_PRE_MAPPED_VRS_ALLELE_VRS2_X
            if alleles
            else TEST_VALID_PRE_MAPPED_VRS_CIS_PHASED_BLOCK,
            post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X
            if alleles
            else TEST_VALID_POST_MAPPED_VRS_CIS_PHASED_BLOCK,
            variant=variant,
            vrs_version="2.0",
            modification_date=date.today(),
            mapped_date=date.today(),
            mapping_api_version="pytest.0.0",
            current=True,
        )
        db.add(mapped_variant)

    db.commit()

    return client.get(f"/api/v1/score-sets/{score_set['urn']}").json()


def publish_score_set(client: TestClient, score_set_urn: str) -> Dict[str, Any]:
    response = client.post(f"/api/v1/score-sets/{score_set_urn}/publish")
    assert response.status_code == 200, f"Could not publish score set {score_set_urn}"

    response_data = response.json()
    return response_data
