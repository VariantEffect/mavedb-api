NON_FIELD_ERRORS = "__all__"


class ValidationError(ValueError, AssertionError):
    def __init__(self, *args: object, triggers=None, custom_loc=None) -> None:
        super().__init__(*args)
        self.custom_loc = custom_loc

        self.triggering_exceptions = triggers
