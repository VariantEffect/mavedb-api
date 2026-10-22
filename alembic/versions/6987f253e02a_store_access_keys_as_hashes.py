"""store access keys as hashes

Revision ID: 6987f253e02a
Revises: d7a3e9c1f5b2
Create Date: 2026-10-21

Replaces each ``access_keys.key_id`` with the hex SHA-256 of the key, and adds ``key_hint``, the key's
last four characters, so listings can tell keys apart. Authentication hashes the presented key and looks
it up by hash, so existing keys keep working. Keys are long and random, so an unsalted SHA-256 suffices.

The hint can only be taken from the key before it is hashed, so this migration fills it.

Can't be downgraded: the original keys can't be recovered from their hashes. Roll back with a database
snapshot.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "6987f253e02a"
down_revision = "d7a3e9c1f5b2"
branch_labels = None
depends_on = None

# One statement, so the hint is read from the key before it is hashed. The unique index on key_id carries
# over: distinct keys have distinct hashes. tests/lib/test_authentication.py runs this against a stored key.
HASH_ACCESS_KEYS = (
    "UPDATE access_keys SET key_hint = right(key_id, 4), key_id = encode(sha256(convert_to(key_id, 'UTF8')), 'hex')"
)


def upgrade() -> None:
    op.add_column("access_keys", sa.Column("key_hint", sa.String(), nullable=True))
    op.execute(HASH_ACCESS_KEYS)


def downgrade() -> None:
    raise NotImplementedError("Access keys can't be restored from their hashes. Restore from a snapshot instead.")
