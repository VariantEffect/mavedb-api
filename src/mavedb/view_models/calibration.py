from typing import Union
from typing_extensions import Self

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models.base.base import BaseModel


class PillarProjectParameters(BaseModel):
    skew: float
    location: float
    scale: float


class PillarProjectParameterSet(BaseModel):
    functionally_altering: PillarProjectParameters
    functionally_normal: PillarProjectParameters
    fraction_functionally_altering: float


class PillarProjectCalibration(BaseModel):
    parameter_sets: list[PillarProjectParameterSet]
    evidence_strengths: list[int]
    thresholds: list[float]
    positive_likelihood_ratios: list[float]
    prior_probability_pathogenicity: float

    @model_validator(mode="after")
    def validate_all_calibrations_have_a_pairwise_companion(self: Self):
        num_es = len(self.evidence_strengths)
        num_st = len(self.thresholds)
        num_plr = len(self.positive_likelihood_ratios)

        if len(set((num_es, num_st, num_plr))) != 1:
            raise ValidationError(
                "Calibration object must provide the same number of evidence strengths, score thresholds, and positive likelihood ratios. "
                "One or more of these provided objects was not the same length as the others."
            )

        return self


Calibration = Union[PillarProjectCalibration]
