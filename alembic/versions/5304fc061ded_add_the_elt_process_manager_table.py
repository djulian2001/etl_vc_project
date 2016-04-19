"""add the elt process manager table

Revision ID: 5304fc061ded
Revises: 9bb09fde359a
Create Date: 2016-04-18 17:11:41.651259

"""

# revision identifiers, used by Alembic.
revision = '5304fc061ded'
down_revision = '9bb09fde359a'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
    	"etl_process_manager",

     	sa.Column( "id", sa.Integer, primary_key=True ),
     	sa.Column( "source_hash", sa.String(64), nullable=False ),
     	sa.Column( "created_at", sa.DateTime, nullable=False ),
     	sa.Column( "updated_at", sa.DateTime, nullable=True ),
     	sa.Column( "deleted_at", sa.DateTime, nullable=True ),
     	sa.Column( "updated_flag", sa.Boolean(), default=False, nullable=False ),

    	sa.Column( "run_status", sa.String(15), default='run in progress',nullable=False ),
    	sa.Column( "started_at", sa.DateTime, nullable=False ),
    	sa.Column( "ended_at", sa.DateTime, nullable=True ),
    	sa.Column( "ending_status", sa.Boolean, default=False, nullable=False ),
    	sa.Column( "emplids_not_processed", sa.Text(), default=None, nullable=True ),
    	mysql_engine ='InnoDB',
   	)


def downgrade():
    op.drop_table("etl_process_manager")
