# dump_rels.py: dumps all "relationships" for the entities out in
# a one-line-per relationship text format.
#
# You can then diff the output before and after a change
# to the newer declarative format and make sure you didn't change the
# nature of any relationships, eg:
#
# git checkout main
# python tests/dump_rels.py > dump_rels_main.txt
# git checkout mybranch
# python tests/dump_rels.py > dump_rels_mybranch.txt
# diff dump_rels_main.txt dump_rels_mybranch.txt
#
# TODO: automate this as a QA check.


from sqlalchemy.orm import mapperlib

from mavedb.models import *  # noqa

mappers = (
    m for r in mapperlib._mapper_registries for m in r.mappers
)

for m in sorted(mappers, key=lambda m: m.entity.__name__):
    for r in sorted(m.relationships, key=lambda r: r.target.name):
        opts = ' '.join(sorted(r.cascade))
        print(f"{m.entity.__name__} {r.key} {r.target.name} {opts}")
