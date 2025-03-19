from typing import Dict, Any


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
