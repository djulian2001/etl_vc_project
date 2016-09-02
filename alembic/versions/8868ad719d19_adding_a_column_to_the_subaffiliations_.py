"""adding a column to the subaffiliations table and data filled for existing records.

Revision ID: 8868ad719d19
Revises: 4480d461c624
Create Date: 2016-09-01 09:15:15.626700

"""

# revision identifiers, used by Alembic.
revision = '8868ad719d19'
down_revision = '4480d461c624'
branch_labels = None
depends_on = None

from alembic import op
from sqlalchemy.sql import table, column
import sqlalchemy as sa

def upgrade():
    # op.add_column('account', sa.Column('last_transaction_date', sa.DateTime))
    op.add_column('subaffiliations', sa.Column('display_title', sa.String(127), nullable=False, default='tbd' ) )
    
    seedTheUpgrade()

def downgrade():
    op.drop_column('subaffiliations', 'display_title')


def seedTheUpgrade():
    
    # create an ad-hoc 
    subaffiliations = table(
        'subaffiliations',
            column( 'code', sa.String(7)),
            column( 'display_title', sa.String(127) ) )

    seedValues = [
        ('BDAF','Faculty Researcher',),
        ('BDRP','Research Professional',),
        ('BDAS','Staff Affiliate',),
        ('BDFC','ASU Faculty Collaborator',),
        ('BDAG','Graduate Affiliate',),
        ('BAPD','Post Doctorate Affiliate',),
        ('BVIP','Institute VIP',),
        ('BDAU','Undergraduate Affiliate',),
        ('BDHV','High School Volunteer',),
        ('NCON','Consultant',),
        ('BDEC','External Collaborator',),
        ('NVOL','Volunteer',),
        ('BEXD','Executive Director',),
        ('BEXI','Interim Executive Director',),
        ('BCOO','Chief Operating Officer',),
        ('BDIR','Department Director',),
        ('BCDR','Center Director',),
        ('BCDI','Interim Center Director',), ]

    for seed in seedValues:
        op.execute(
            subaffiliations.update().where(
                subaffiliations.c.code==seed[0] ).values(
                    display_title=seed[1] ) )


