from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import bioPublicTables as bio_model
import peopleSoftTables as ps_model

# build the connection
engine_source = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public_ps', echo=True)
engine_target = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public', echo=True)

# bio.PsPublic.metadata.bind = engine_source
# bio.PsPublic.metadata.create_all(engine_target)

# BioPublic.metadata.drop_all(engine_target)
bio_model.BioPublic.metadata.create_all(engine_target)

Source_Session = sessionmaker(bind=engine_source)
# Target_Session = sessionmaker(bind=engine_target)

ps_model.PsPublic.metadata.create_all(engine_source)

people_session = Source_Session()

s_people = people_session.query(People).all()



people_session.close()
