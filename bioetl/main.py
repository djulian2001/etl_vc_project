# from sharedProcesses import resetUpdatedFlag
# from sqlalchemy.orm.exc import NoResultFound
import sys
import logging

from app.appsetup import AppSetup, LoggingSetup
from etlProcess import EtlProcess

def main():
	"""
		The main method that starts the application, the logging should be initiated at this point
		From this point the application will set up a class of AppSetup that will manage the setup
		of the database connections, ssh tunnel, the session factories, 
	"""
	logger.info( "Application setup, bioetl: BEGINNING" )
	try:
		etlSetup = AppSetup("asutobio")
	except Exception as e:
		logger.error( 'Application Failed to setup correctly:', exc_info=True )
		cleanUp( e )

	logger.info( "Application setup, bioetl: COMPLETED" )

	# from etlProcess import EtlProcess
	logger.info( "Application ETL Process, bioetl: BEGINNING")
	try:
		etl = EtlProcess( etlSetup )
		etl.runProcesses()
		logger.info( "Application ETL Process, bioetl: COMPLETED")

	except Exception as e:
		cleanUp( e, etlSetup )
	else:
		cleanUp( None, etlSetup )

def cleanUp(e , appSetup=None):
	"""
		Try/Catch...  As errors pop up, use the Exception passed by python and or sqlalchemy
		as how to handle the error.  Wrap the code block as specific as possible to either 
		handle the Exception or log the error ( TO_DO )
		@False: application failed to run, an Exception was raised
		@True: the application ran, no Exception was raised
	"""
	errors = []
	
	try:
		appSetup.cleanUp()
	except Exception as eTarget:
		errors.append( eTarget )

	for i in errors:
		"""
			In the event that there is an issue closing the various connections that are established
			when the application is in various states.
			The else block runs to report the issue that cause the application to close, via an error
			or because the end of the etl process occured.
		"""
		logger.error( i, exc_info=True )
	else:
		if e:
			logger.error( 'Application shutdown, bioetl - FAILURE: terminated prematurely.', exc_info=True )
			sys.exit(1)
		else:
			logger.info( "Application shutdown, bioetl - COMPLETED: cleanly" )
			sys.exit(0)


if __name__=="__main__":
	LoggingSetup()
	logger = logging.getLogger(__name__)

	main()