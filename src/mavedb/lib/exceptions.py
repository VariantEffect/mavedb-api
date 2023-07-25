import datetime
from decimal import Decimal

NON_FIELD_ERRORS = "__all__"

_PROTECTED_TYPES = (type(None), int, float, Decimal, datetime.datetime, datetime.date, datetime.time)


def is_protected_type(obj):
    """Determine if the object instance is of a protected type.

    Objects of protected types are preserved as-is when passed to
    force_str(strings_only=True).
    """
    return isinstance(obj, _PROTECTED_TYPES)


def force_str(s, encoding="utf-8", strings_only=False, errors="strict"):
    """
    Return a str object representing arbitrary object s.

    Bytestrings are treated using the encoding codec.

    If strings_only is True, some non-string-like objects are not converted.
    """
    # Handle the common case first for performance reasons.
    if issubclass(type(s), str):
        return s
    if strings_only and is_protected_type(s):
        return s
    try:
        if isinstance(s, bytes):
            s = str(s, encoding, errors)
        else:
            s = str(s)
    except UnicodeDecodeError as e:
        # raise DjangoUnicodeDecodeError(s, *e.args)
        raise e
    return s


class ValidationError(Exception):
    """An error while validating data."""

    def __init__(self, message, code=None, params=None):
        """
        The `message` argument can be a single error, a list of errors, or a
        dictionary that maps field names to lists of errors. What we define as
        an "error" can be either a simple string or an instance of
        ValidationError with its message attribute set, and what we define as
        list or dictionary can be an actual `list` or `dict` or an instance
        of ValidationError with its `error_list` or `error_dict` attribute set.
        """

        # PY2 can't pickle naive exception: http://bugs.python.org/issue1692335.
        super(ValidationError, self).__init__(message, code, params)

        if isinstance(message, ValidationError):
            if hasattr(message, "error_dict"):
                message = message.error_dict
            # PY2 has a `message` property which is always there so we can't
            # duck-type on it. It was introduced in Python 2.5 and already
            # deprecated in Python 2.6.
            elif not hasattr(message, "message" if six.PY3 else "code"):
                message = message.error_list
            else:
                message, code, params = message.message, message.code, message.params

        if isinstance(message, dict):
            self.error_dict = {}
            for field, messages in message.items():
                if not isinstance(messages, ValidationError):
                    messages = ValidationError(messages)
                self.error_dict[field] = messages.error_list

        elif isinstance(message, list):
            self.error_list = []
            for message in message:
                # Normalize plain strings to instances of ValidationError.
                if not isinstance(message, ValidationError):
                    message = ValidationError(message)
                if hasattr(message, "error_dict"):
                    self.error_list.extend(sum(message.error_dict.values(), []))
                else:
                    self.error_list.extend(message.error_list)

        else:
            self.message = message
            self.code = code
            self.params = params
            self.error_list = [self]

    @property
    def message_dict(self):
        # Trigger an AttributeError if this ValidationError
        # doesn't have an error_dict.
        getattr(self, "error_dict")

        return dict(self)

    @property
    def messages(self):
        if hasattr(self, "error_dict"):
            return sum(dict(self).values(), [])
        return list(self)

    def update_error_dict(self, error_dict):
        if hasattr(self, "error_dict"):
            for field, error_list in self.error_dict.items():
                error_dict.setdefault(field, []).extend(error_list)
        else:
            error_dict.setdefault(NON_FIELD_ERRORS, []).extend(self.error_list)
        return error_dict

    def __iter__(self):
        if hasattr(self, "error_dict"):
            for field, errors in self.error_dict.items():
                yield field, list(ValidationError(errors))
        else:
            for error in self.error_list:
                message = error.message
                if error.params:
                    message %= error.params
                yield force_str(message)

    def __str__(self):
        if hasattr(self, "error_dict"):
            return repr(dict(self))
        return repr(list(self))

    def __repr__(self):
        return "ValidationError(%s)" % self


class OpenIDConnectException(Exception):
    """Raised when OpenID login flow fails."""

    pass


class AmbiguousIdentifierError(ValueError):
    """Raised when a user tries to create a publication with an ambiguous identifier"""

    pass


class NonexistentIdentifierError(ValueError):
    """Raised when a user tries to create a publication with a non-existent identifier"""

    pass
