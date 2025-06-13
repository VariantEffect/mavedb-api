from mavedb.lib.exceptions import NonexistentOrcidUserError


async def awaitable_exception() -> Exception:
    return Exception()


def callable_nonexistent_orcid_user_exception():
    raise NonexistentOrcidUserError()
