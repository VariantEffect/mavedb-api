class MappingDataDoesntExistException(ValueError):
    pass


#: Exceptions meaning a variant has nothing to annotate, as opposed to something going wrong while
#: annotating it. Every caller that reports failures has to tell the two apart: the streaming endpoints
#: emit an expected absence as a null annotation rather than an error record, and the corpus sweep counts
#: it as not-applicable rather than a defect. Adding an entry here changes both, and they must agree —
#: a sweep that treated an expected absence as a failure would bury real defects in noise.
EXPECTED_ABSENCE_EXCEPTIONS = (MappingDataDoesntExistException,)
