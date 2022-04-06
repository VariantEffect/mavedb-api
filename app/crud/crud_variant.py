from app.crud.base import CrudBase
from app.models.variant import Variant
from app.view_models.variant import VariantCreate, VariantUpdate


class CrudVariant(CrudBase[Variant, VariantCreate, VariantUpdate]):
    ...


variant = CrudVariant(Variant)
