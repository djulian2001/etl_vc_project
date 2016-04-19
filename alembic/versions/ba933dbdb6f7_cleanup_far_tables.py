"""cleanup far tables

Revision ID: ba933dbdb6f7
Revises: 6fdfc671c216
Create Date: 2016-04-18 15:23:56.874194

"""

# revision identifiers, used by Alembic.
revision = 'ba933dbdb6f7'
down_revision = '6fdfc671c216'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    """The following drops are the far data which will no longer be supported."""
    op.drop_table("far_shortstories")
    op.drop_table("far_encyclopediaarticles")
    op.drop_table("far_bookreviews")
    op.drop_table("far_bookchapters")
    op.drop_table("far_editedbooks")
    op.drop_table("far_refereedarticles")
    op.drop_table("far_authoredbooks")
    op.drop_table("far_conferenceproceedings")
    


def downgrade():
    """To bring these back, it will be done in a new reversion script as an upgrade
        reference the models.depricated_farmodels file for the orm declarative objects.
    """
    pass
