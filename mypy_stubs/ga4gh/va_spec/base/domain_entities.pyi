from pydantic import RootModel

from ...core.models import Element, MappableConcept
from .enums import MembershipOperator

class ConditionSet(Element):
    conditions: list[MappableConcept]
    membershipOperator: MembershipOperator

class Condition(RootModel):
    root: ConditionSet | MappableConcept
