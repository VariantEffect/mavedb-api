from mavedb.view_models.contributor import ContributorCreate


# Test valid contributor
def test_create_access_key():
    orcid = "1111-2222-3333-4444"
    contributor = ContributorCreate(
        orcid_id=orcid,
    )
    assert contributor.orcid_id == orcid
