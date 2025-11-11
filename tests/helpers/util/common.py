from typing import Dict, Any
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


def deepcamelize(data: Any) -> Any:
    if isinstance(data, dict):
        return {camelize(k): deepcamelize(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [deepcamelize(item) for item in data]
    else:
        return data
