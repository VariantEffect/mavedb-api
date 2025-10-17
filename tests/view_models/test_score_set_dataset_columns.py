from mavedb.view_models.score_set_dataset_columns import DatasetColumnMetadata, SavedDatasetColumns
from tests.helpers.constants import TEST_SCORE_SET_DATASET_COLUMNS

def test_score_set_dataset_columns():
    score_set_dataset_columns = TEST_SCORE_SET_DATASET_COLUMNS.copy()

    for k, v in score_set_dataset_columns['score_columns_metadata'].items():
        score_set_dataset_columns['score_columns_metadata'][k] = DatasetColumnMetadata.model_validate(v)
    for k, v in score_set_dataset_columns['count_columns_metadata'].items():
        score_set_dataset_columns['count_columns_metadata'][k] = DatasetColumnMetadata.model_validate(v)

    saved_score_set_dataset_columns = SavedDatasetColumns.model_validate(score_set_dataset_columns)

    assert saved_score_set_dataset_columns.score_columns_metadata == score_set_dataset_columns['score_columns_metadata']
    assert saved_score_set_dataset_columns.count_columns_metadata == score_set_dataset_columns['count_columns_metadata']
    assert saved_score_set_dataset_columns.score_columns == score_set_dataset_columns['score_columns']
    assert saved_score_set_dataset_columns.count_columns == score_set_dataset_columns['count_columns']
