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
		cleanUp(e)

	logger.info( "Application setup, bioetl: COMPLETED" )

	# from etlProcess import EtlProcess
	logger.info( "Application ETL Process, bioetl: BEGINNING")
	try:
		etl = EtlProcess( etlSetup )
		etl.runProcesses()
		logger.info( "Application ETL Process, bioetl: COMPLETED")

	except Exception as e:
		cleanUp( etlSetup, e )
	else:
		cleanUp( etlSetup, None )

def cleanUp(appSetup, e):
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

###############################################################################
# 
#   File Import:  jobLogProcessing
def runStuff():
	import jobLogProcessing

	if True:
		logger.info("Starting source database query.")
		# srcJobsLog = jobLogProcessing.getSourceJobsLog( sesSource )
		logger.info("Finished source database query.")

		iJobLog = 1
		
		logger.info("Starting source database query.")
	# 	for srcJobLog in srcJobsLog:

	# 		# print( type(srcJobLog) )
	# ##############################
	# # I STOPPED HERE.....
	# ##############################

	# 		try:
	# 			processedJobLog = jobLogProcessing.processJobLog( srcJobLog, sesTarget )
	# 		except NoResultFound as e:
	# 			logger.warning( 'Constraint Failed to match a record from {o.schema}.{o.__tablename__};  Record @ emplid: {o.emplid}, deptid: {o.deptid}, jobcode: {o.jobcode}, effdt: {o.effdt}, action: {o.action}, action_reason: {o.action_reason};'.format(o=srcJobLog) )			
	# 		except Exception as e:
	# 			logger.error( 'Code Failure:', exc_info=True )
	# 			cleanUp(None)
			
	# 		if processedJobLog:
	# 			sesTarget.add( processedJobLog )
	# 			if iJobLog % 1000 == 0:
	# 				try:
	# 					sesTarget.flush()
	# 				except sqlalchemy.exc.IntegrityError as e:
	# 					sesTarget.rollback()
	# 					raise e
	# 			iJobLog += 1
	# 	try:
	# 		sesTarget.commit()
	# 	except sqlalchemy.exc.IntegrityError as e:
	# 		sesTarget.rollback()
	# 		raise e

	# 	tgtMissingJobsLog = jobLogProcessing.getTargetJobsLog( sesTarget )

	# 	iRemoveJobLog = 1
	# 	for tgtMissingJobLog in tgtMissingJobsLog:
	# 		removeJobLog = jobLogProcessing.softDeleteJobLog( tgtMissingJobLog, srcJobsLog )
	# 		if removeJobLog:
	# 			sesTarget.add( removeJobLog )
	# 			if iRemoveJobLog % 1000 == 0:
	# 				try:
	# 					sesTarget.flush()
	# 				except sqlalchemy.exc.IntegrityError as e:
	# 					sesTarget.rollback()
	# 					raise e
	# 			iRemoveJobLog += 1
	# 	try:
	# 		sesTarget.commit()
	# 	except sqlalchemy.exc.IntegrityError as e:
	# 		sesTarget.rollback()
	# 		raise e
		logger.info("End of jobLogProcessing")
		logger.info("Starting EtlProcess...")
		etl = EtlProcess(sesSource)

#	End of jobLogProcessing
###############################################################################

	cleanUp( None )

if __name__=="__main__":
	LoggingSetup()
	logger = logging.getLogger(__name__)

	main()