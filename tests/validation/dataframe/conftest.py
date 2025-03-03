import pandas as pd
import pytest
from unittest import mock, TestCase

from mavedb.lib.validation.constants.general import (
    hgvs_nt_column,
    hgvs_pro_column,
    hgvs_splice_column,
    guide_sequence_column,
    required_score_column,
)
from tests.helpers.constants import TEST_CDOT_TRANSCRIPT


@pytest.fixture
def mocked_data_provider_class_attr(request):
    """
    Sets the `human_data_provider` attribute on the class from the requesting
    test context to the `data_provider` fixture. This allows fixture use across
    the `unittest.TestCase` class.
    """
    data_provider = mock.Mock()
    data_provider._get_transcript.return_value = TEST_CDOT_TRANSCRIPT
    request.cls.mocked_human_data_provider = data_provider


# Special DF Test Case that contains dummy data for tests below
@pytest.mark.usefixtures("mocked_data_provider_class_attr")
class DfTestCase(TestCase):
    def setUp(self):
        self.dataframe = pd.DataFrame(
            {
                hgvs_nt_column: ["g.1A>G", "g.1A>T"],
                hgvs_splice_column: ["c.1A>G", "c.1A>T"],
                hgvs_pro_column: ["p.Met1Val", "p.Met1Leu"],
                guide_sequence_column: ["AG", "AG"],
                required_score_column: [1.0, 2.0],
                "extra": [12.0, 3.0],
                "count1": [3.0, 5.0],
                "count2": [9, 10],
                "extra2": ["pathogenic", "benign"],
                "mixed_types": ["test", 1.0],
                "null_col": [None, None],
            }
        )
