"""cleanup far tables, part2

Revision ID: 9bb09fde359a
Revises: ba933dbdb6f7
Create Date: 2016-04-18 15:47:03.481431

"""

# revision identifiers, used by Alembic.
revision = '9bb09fde359a'
down_revision = 'ba933dbdb6f7'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.drop_table("far_nonrefereedarticles")
    op.drop_table("far_evaluations")


def downgrade():
    pass
