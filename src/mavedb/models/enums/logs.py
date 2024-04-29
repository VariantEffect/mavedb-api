import enum


class LogType(enum.Enum):
    api_request = "api_request"

class Method(enum.Enum):
    delete = "delete"
    get = "get"
    post = "post"
    put = "put"

class Source(enum.Enum):
    docs = "docs"
    other = "other"
    web = "web"
