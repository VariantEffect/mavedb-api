class HGVSError(Exception):
    pass

class HGVSDataNotAvailableError(HGVSError):
    pass

class HGVSInternalError(HGVSError):
    pass

class HGVSInvalidIntervalError(HGVSError):
    pass

class HGVSInvalidVariantError(HGVSError):
    pass

class HGVSNormalizationError(HGVSError):
    pass

class HGVSParseError(HGVSError):
    pass

class HGVSUnsupportedOperationError(HGVSError):
    pass

class HGVSUsageError(HGVSError):
    pass

class HGVSVerifyFailedError(HGVSError):
    pass
