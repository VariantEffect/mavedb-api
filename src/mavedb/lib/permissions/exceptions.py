class PermissionException(Exception):
    def __init__(self, http_code: int, message: str):
        self.http_code = http_code
        self.message = message
