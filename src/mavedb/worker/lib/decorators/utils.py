import os


def is_test_mode() -> bool:
    """Check if the application is running in test mode based on the MAVEDB_TEST_MODE environment variable.

    Returns:
        bool: True if in test mode, False otherwise.
    """
    # Although not ideal, we use an environment variable to detect whether
    # the application is in test mode. In the context of decorators, test
    # mode makes them no-ops to facilitate unit testing without side effects.
    #
    # This is necessary because decorators are applied at import time, making
    # it difficult to mock their behavior in tests when they must be imported
    # up front and provided to the ARQ worker.
    #
    # This pattern allows us to control decorator behavior in tests without
    # altering production code paths.
    return os.getenv("MAVEDB_TEST_MODE") == "1"
