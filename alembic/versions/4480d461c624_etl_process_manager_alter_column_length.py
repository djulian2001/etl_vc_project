"""etl process manager alter column length

Revision ID: 4480d461c624
Revises: 5304fc061ded
Create Date: 2016-04-19 12:14:59.849457

"""

# revision identifiers, used by Alembic.
revision = '4480d461c624'
down_revision = '5304fc061ded'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
	op.alter_column( 'etl_process_manager', 'run_status', nullable=False, server_default='ETL process starting', type_=sa.String(63)  )


def downgrade():
	op.alter_column( 'etl_process_manager', 'run_status', nullable=False, server_default='run in progress', type_=sa.String(15)  )
    
