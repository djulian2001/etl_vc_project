from sharedProcesses import resetUpdatedFlag
from app.appsetup import AppSetup

from sqlalchemy.orm.exc import NoResultFound

bioetlAppRun = AppSetup("asutobio")

sesSource = bioetlAppRun.getSourceSession()
sesTarget = bioetlAppRun.getTargetSession()
test_logger = bioetlAppRun.getLogger( __name__ )

print( sesSource, type( sesSource ) )
print( sesTarget, type( sesTarget ) )
print( test_logger, type( test_logger ), dir( test_logger ) )


def cleanUp(e):
	"""
		Try/Catch...  As errors pop up, use the Exception passed by python and or sqlalchemy
		as how to handle the error.  Wrap the code block as specific as possible to either 
		handle the Exception or log the error ( TO_DO )
		@False: application failed to run, an Exception was raised
		@True: the application ran, no Exception was raised
	"""
	import sys

	sesTarget.close()
	sesSource.close()
	bioetlAppRun.cleanUp()
	if e:
		print type(e), e
		sys.exit(1)
	else:
		sys.exit(0)




###############################################################################
# 
#   File Import:  jobLogProcessing

import jobLogProcessing

if True:
	test_logger.info("Starting source database query.")
	srcJobsLog = jobLogProcessing.getSourceJobsLog( sesSource )
	test_logger.info("Finished source database query.")

	iJobLog = 1
	
	test_logger.info("Starting source database query.")
	for srcJobLog in srcJobsLog:

		# print( type(srcJobLog) )
##############################
# I STOPPED HERE.....
##############################

		try:
			processedJobLog = jobLogProcessing.processJobLog( srcJobLog, sesTarget )
		except NoResultFound as e:
			test_logger.warning( 'Constraint Failed to match a record from {o.schema}.{o.__tablename__};  Record @ emplid: {o.emplid}, deptid: {o.deptid}, jobcode: {o.jobcode}, effdt: {o.effdt}, action: {o.action}, action_reason: {o.action_reason};'.format(o=srcJobLog) )			
		except Exception as e:
			test_logger.error( 'Code Failure:', exc_info=True )
			cleanUp(None)
		
		if processedJobLog:
			sesTarget.add( processedJobLog )
			if iJobLog % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iJobLog += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingJobsLog = jobLogProcessing.getTargetJobsLog( sesTarget )

	iRemoveJobLog = 1
	for tgtMissingJobLog in tgtMissingJobsLog:
		removeJobLog = jobLogProcessing.softDeleteJobLog( tgtMissingJobLog, srcJobsLog )
		if removeJobLog:
			sesTarget.add( removeJobLog )
			if iRemoveJobLog % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveJobLog += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of jobLogProcessing
###############################################################################

cleanUp( None )
