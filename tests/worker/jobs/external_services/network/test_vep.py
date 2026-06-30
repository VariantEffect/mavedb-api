# ruff: noqa: E402

import pytest

pytest.importorskip("arq")

from sqlalchemy import select

from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.job_pipeline import JobStatus, PipelineStatus
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestE2EPopulateVepForScoreSet:
    """End-to-end test for VEP functional consequence prediction against the real Ensembl API."""

    async def test_populate_vep_e2e(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_populate_vep_run_pipeline,
        sample_populate_vep_pipeline,
        setup_sample_alleles_for_vep,
    ):
        """Enqueue the VEP job, run the worker, and verify the allele's consequence and annotation."""
        variant, allele = setup_sample_alleles_for_vep

        await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run_pipeline.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.SUCCEEDED

        live = session.scalars(
            select(VepAlleleConsequence).where(
                VepAlleleConsequence.allele_id == allele.id,
                VepAlleleConsequence.current,
            )
        ).one()
        assert live.functional_consequence is not None
        assert live.access_date is not None

        # Events are allele-keyed now; the variant resolves its status through the live link.
        event = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.allele_id == allele.id,
                AnnotationEvent.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert event.disposition == Disposition.PRESENT

    async def test_populate_vep_e2e_with_recoder_path(
        self,
        session,
        arq_redis,
        arq_worker,
        sample_populate_vep_run_pipeline,
        sample_populate_vep_pipeline,
        setup_sample_protein_allele_for_vep,
    ):
        """VEP job uses Variant Recoder for a protein HGVS (NP_ accession) that VEP cannot resolve directly.

        NP_009225.1:p.Val1696His is a BRCA1 protein variant that VEP's /vep/human/hgvs endpoint
        does not return a consequence for.  The job must fall back to Variant Recoder, recode it
        to a genomic HGVS, and then re-query VEP with the recoded string.
        """
        variant, allele = setup_sample_protein_allele_for_vep

        await arq_redis.enqueue_job("populate_vep_for_score_set", sample_populate_vep_run_pipeline.id)
        await arq_worker.async_run()
        await arq_worker.run_check()

        session.refresh(sample_populate_vep_run_pipeline)
        assert sample_populate_vep_run_pipeline.status == JobStatus.SUCCEEDED

        session.refresh(sample_populate_vep_pipeline)
        assert sample_populate_vep_pipeline.status == PipelineStatus.SUCCEEDED

        live = session.scalars(
            select(VepAlleleConsequence).where(
                VepAlleleConsequence.allele_id == allele.id,
                VepAlleleConsequence.current,
            )
        ).one()
        assert live.functional_consequence is not None
        assert live.access_date is not None

        # Events are allele-keyed now; the variant resolves its status through the live link.
        event = session.scalars(
            select(AnnotationEvent).where(
                AnnotationEvent.allele_id == allele.id,
                AnnotationEvent.annotation_type == AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            )
        ).one()
        assert event.disposition == Disposition.PRESENT
