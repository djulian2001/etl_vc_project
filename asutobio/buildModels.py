from sqlalchemy import *
from models.biopsmodels import *

# target engine
targetDbUser = 'app_asutobioetl'
targetDbPw = 'forthegipperNotReagan4show'
targetDbHost = 'dbdev.biodesign.asu.edu'
targetDbName = 'bio_ps'
engTargetString = 'mysql+mysqldb://%s:%s@%s/%s' % (targetDbUser, targetDbPw, targetDbHost, targetDbName)
engineTarget = create_engine( engTargetString, echo=True )

# BioPublic.metadata.drop_all(engineTarget)

BioPs.metadata.create_all(engineTarget)