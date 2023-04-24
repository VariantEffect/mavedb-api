from uuid import uuid4


def generate_temp_urn():
    """
    Generate a new temporary URN for an unpublished entity (experiment set, experiment, or score set).
    """
    return f"tmp:{uuid4()}"
