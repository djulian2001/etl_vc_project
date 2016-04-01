from sharedProcesses import resetUpdatedFlag
from app.appsetup import AppSetup, LoggingSetup

from sqlalchemy.orm.exc import NoResultFound


def main():
	# import logging
	# LoggingSetup()
	# logger = logging.getLogger(__name__)

	logger.info("Begin bioetl application setup")
	try:
		bioetlAppRun = AppSetup("asutobio")
	except Exception as e:
		logger.warning( 'Application Failed to setup correctly:', exc_info=True )
		cleanUp(e)

	logger.info("Completed bioetl application setup")

	sesSource = bioetlAppRun.getSourceSession()
	sesTarget = bioetlAppRun.getTargetSession()

	runStuff()

def cleanUp(e):
	"""
		Try/Catch...  As errors pop up, use the Exception passed by python and or sqlalchemy
		as how to handle the error.  Wrap the code block as specific as possible to either 
		handle the Exception or log the error ( TO_DO )
		@False: application failed to run, an Exception was raised
		@True: the application ran, no Exception was raised
	"""
	import sys
	errors = []

	try:
		sesTarget.close()
	except Exception as eTarget:
		errors.append( eTarget )

	try:
		sesSource.close()
	except Exception as eTarget:
		errors.append( eTarget )
	
	try:
		bioetlAppRun.cleanUp()
	except Exception as eTarget:
		errors.apppend( eTarget )

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
			logger.error( 'BIOETL application terminated prematurely.', exc_info=True )
			sys.exit(1)
		else:
			logger.info( "BIOETL application process - completed cleanly" )
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
		from etlProcess import EtlProcess
		etl = EtlProcess()

#	End of jobLogProcessing
###############################################################################

	cleanUp( None )

if __name__=="__main__":
	import logging
	LoggingSetup()
	logger = logging.getLogger(__name__)

	main()