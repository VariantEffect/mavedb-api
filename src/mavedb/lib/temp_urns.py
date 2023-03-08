from uuid import uuid4


def generate_temp_urn():
    return f'tmp:{uuid4()}'
