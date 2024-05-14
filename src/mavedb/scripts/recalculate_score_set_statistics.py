"""
Recalculate and update statistics for all score sets.

Usage:
```
python3 -m mavedb.scripts.recalculate_score_set_statistics
```
"""

import argparse
import logging


from sqlalchemy import select

from mavedb.lib.score_sets import calculate_score_set_statistics
from mavedb.lib.script_environment import init_script_environment
from mavedb.models.score_set import ScoreSet

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    prog='recalculate_score_set_statistics',
    description='Calculate and store statistics for one score set or all score sets.'
)
parser.add_argument("-urn", help="URN of the score set to update")
args = parser.parse_args()

db = init_script_environment()

if args.urn is not None:
    score_sets = db.scalars(select(ScoreSet).where(ScoreSet.urn == args.urn)).all()
else:
    score_sets = db.scalars(select(ScoreSet)).all()

for score_set in score_sets:
    statistics = calculate_score_set_statistics(score_set)
    print(statistics)
    score_set.statistics = statistics
    db.add(score_set)
    db.commit()
