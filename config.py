# the config.py for the etl application used by bits to move peoplesoft data into a biodesign database.
import os
basedir = os.path.abspath(os.path.dirname(__file__))

# our application will have multipule database uri: 3 will be specified...
SQLALCHEMY_BATABASE_URI = ""
SQLALCHEMY_MIGRATE_REPO = os.path.join(basedir, "<name-of-db-repo-directory>")

# other configurations...