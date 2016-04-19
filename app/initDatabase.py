from sqlalchemy.engine.reflection import Inspector

from appsetup import AppSetup

appSetup = AppSetup("DbInit")

inspectDb = Inspector.from_engine( appSetup.targetEngine )

if not inspectDb.get_table_names():
	"""
		Create the database using the current models within models.biopublicmodels 
		using the declarative base.  When the database is empty.
	"""	
	from models.biopublicmodels import BioPublic

	BioPublic.metadata.create_all( appSetup.targetEngine )
	
