"""add and alter a column in the person_department_subaffiliations table

Revision ID: 5573e37ff157
Revises: 8868ad719d19
Create Date: 2016-09-02 10:57:05.111571

"""

# revision identifiers, used by Alembic.
revision = '5573e37ff157'
down_revision = '8868ad719d19'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    
    op.alter_column( 'person_department_subaffiliations', 'description', new_column_name='dwps_description', type_=sa.String(47) )
    
    op.add_column('person_department_subaffiliations', sa.Column('description', sa.String(127), nullable=False, default='ASU' ) )
    


def downgrade():

    op.drop_column( 'person_department_subaffiliations', 'description' )

    op.alter_column( 'person_department_subaffiliations', 'dwps_description', new_column_name='description', type_=sa.String(47) )
