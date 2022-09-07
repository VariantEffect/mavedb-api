from datetime import date

from sqlalchemy import Column, Date, Integer, String

from app.db.base import Base


class ReferenceGenome(Base):
    # __tablename__ = 'genome_referencegenome'
    __tablename__ = 'reference_genomes'

    id = Column(Integer, primary_key=True, index=True)
    genome_id_id = Column(Integer, nullable=True)  # , ForeignKey('target_gene.id'), nullable=True)
    # genome_id = relationship('Genome', back_populates='reference_maps')
    short_name = Column(String, nullable=False)
    organism_name = Column(String, nullable=False)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
