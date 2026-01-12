import json
from typing import Any, Dict

from humps import camelize


def update_expected_response_for_created_resources(
    expected_response: Dict[str, Any], created_experiment: Dict[str, Any], created_score_set: Dict[str, Any]
) -> Dict[str, Any]:
    expected_response.update({"urn": created_score_set["urn"]})
    expected_response["experiment"].update(
        {
            "urn": created_experiment["urn"],
            "experimentSetUrn": created_experiment["experimentSetUrn"],
            "scoreSetUrns": [created_score_set["urn"]],
        }
    )

    return expected_response


def dummy_attributed_object_from_dict(properties: dict[str, Any], recursive=False):
    class Object(object):
        pass

    attr_obj = Object()
    for k, v in properties.items():
        if recursive and k in recursive:
            if isinstance(v, dict):
                attr_obj.__setattr__(k, dummy_attributed_object_from_dict(v, v.keys()))
            elif isinstance(v, list):
                attr_obj.__setattr__(k, [dummy_attributed_object_from_dict(d, d.keys()) for d in v])
            else:
                attr_obj.__setattr__(k, v)
        else:
            attr_obj.__setattr__(k, v)

    return attr_obj


def parse_ndjson_response(response):
    """Parse NDJSON response from streaming annotated-variants endpoints."""
    response_data = []
    for line in response.text.strip().split("\n"):
        if line.strip():
            variant_data = json.loads(line)
            response_data.append(variant_data)

    return response_data


def deepcamelize(data: Any) -> Any:
    if isinstance(data, dict):
        return {camelize(k): deepcamelize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [deepcamelize(item) for item in data]
    else:
        return data


def create_failing_side_effect(exception, original_method, fail_on_call=1):
    """
    Create a side effect function that fails on a specific call number, then delegates to original method.

    Args:
        exception: The exception to raise on the failing call
        original_method: The original method to delegate to after the failure
        fail_on_call: Which call number should fail (1-indexed, defaults to first call)

    Returns:
        A callable that can be used as a side_effect in mock.patch

    Example:
        with patch.object(session, "execute", side_effect=create_failing_side_effect(
            SQLAlchemyError("DB Error"), session.execute
        )):
            # First call will raise SQLAlchemyError, subsequent calls work normally
            pass
    """
    call_count = 0

    def side_effect_function(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == fail_on_call:
            raise exception
        return original_method(*args, **kwargs)

    return side_effect_function
