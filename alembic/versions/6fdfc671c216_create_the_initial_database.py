"""create the initial database

Revision ID: 6fdfc671c216
Revises: 
Create Date: 2016-04-18 12:03:11.037519

"""

# revision identifiers, used by Alembic.
revision = '6fdfc671c216'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa

def upgrade():
	"""
		The database is initiated using the app/initDatabase.py file.  This file is to be run
		one time and then the rest of the datbase version(s) changes, etc will be using 
		alembic versions to push changes onto the database(s).
	"""
	pass

def downgrade():
    pass
