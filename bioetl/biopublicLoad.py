from sharedProcesses import resetUpdatedFlag
from app.connectdb import EtlConnections

bioetlAppRun = EtlConnections("asutobio")

sesSource = bioetlAppRun.getSourceSession()
sesTarget = bioetlAppRun.getTargetSession()

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
# 	The Biodesign Defined subaffiliations.  There is BI aspects of this data
#		that warrents that it be scoped.  Reference table so ok to load prior
#		to the people data, and must be loaded prior to the 
#   File Import:  subAffiliationProcessing

import subAffiliationProcessing

if True:
	resetUpdatedFlag( sesTarget, "subaffiliations" )
	
	srcSubAffiliations = subAffiliationProcessing.getSourceSubAffiliations()

	iSubAffiliation = 1
	for srcSubAffiliation in srcSubAffiliations:
		processedsubAffiliation = subAffiliationProcessing.processSubAffiliation( srcSubAffiliation, sesTarget )

		sesTarget.add( processedsubAffiliation )

		if iSubAffiliation % 1000 == 0:
			try:
				sesTarget.flush()
			except sqlalchemy.exc.IntegrityError as e:
				sesTarget.rollback()
				raise e
		iSubAffiliation += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingSubAffiliations = subAffiliationProcessing.getTargetSubAffiliations( sesTarget )

	iRemoveSubAffiliation = 1
	for tgtMissingSubAffiliation in tgtMissingSubAffiliations:
		removeSubAffiliation = subAffiliationProcessing.softDeleteSubAffiliation( tgtMissingSubAffiliation, srcSubAffiliations )

		if removeSubAffiliation:
			sesTarget.add( removeSubAffiliation )

			if iRemoveSubAffiliation % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveSubAffiliation += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	resetUpdatedFlag( sesTarget, "subaffiliations" )

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of subAffiliationProcessing
###############################################################################



###############################################################################
# Load the asu data warehouse people table in into the final destination:
#	mysql:
#		bio_public.people to bio_public.people
# 

import personProcessing
import personWebProfileProcessing
import personExternalLinkProcessing

if True:
	resetUpdatedFlag( sesTarget, "people" )
	resetUpdatedFlag( sesTarget, "person_externallinks" )
	resetUpdatedFlag( sesTarget, "person_webprofile" )

	srcPeople = personProcessing.getSourcePeople( sesSource )

	iPerson = 1
	for srcPerson in srcPeople:
		# try:
		personStatus = personProcessing.processPerson( srcPerson, sesTarget )
		# except TypeError as e:
			# print 1,e  # replace this with 'continue'
		# else:
		sesTarget.add( personStatus )
		sesTarget.flush()

		processedpersonWebProfile = personWebProfileProcessing.processPersonWebProfile( srcPerson, sesTarget )
		if processedpersonWebProfile:
			sesTarget.add( processedpersonWebProfile )

		processedpersonExternalLink = personExternalLinkProcessing.processPersonExternalLink( srcPerson, sesTarget )
		if processedpersonExternalLink:
			sesTarget.add( processedpersonExternalLink )

		if iPerson % 1000 == 0:
			try:
				sesTarget.flush()
			except sqlalchemy.exc.IntegrityError as e:
				sesTarget.rollback()
				raise e
			except RuntimeError as e:
				raise e
		iPerson += 1
	
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingPeople = personProcessing.getTargetPeople( sesTarget )

	iMissingPerson = 1
	for tgtMissingPerson in tgtMissingPeople:
		personMissing = personProcessing.softDeletePerson( tgtMissingPerson, srcPeople )
		if personMissing:
			sesTarget.add( personMissing )
			if iMissingPerson % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iMissingPerson += 1
	
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	###############################################################################
	# 
	#   personWebProfileProcessing

	tgtMissingPersonWebProfiles = personWebProfileProcessing.getTargetPersonWebProfiles( sesTarget )

	iMissingPersonWebProfile = 1
	for tgtMissingPersonWebProfile in tgtMissingPersonWebProfiles:
		removePersonWebProfile = personWebProfileProcessing.softDeletePersonWebProfile( tgtMissingPersonWebProfile, srcPeople )
		if removePersonWebProfile:
			sesTarget.add( removePersonWebProfile )
			if iMissingPersonWebProfile % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iMissingPersonWebProfile += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	# 	End of personWebProfileProcessing
	###############################################################################

	###############################################################################
	# 
	#   File Import:  personExternalLinkProcessing

	tgtMissingPersonExternalLinks = personExternalLinkProcessing.getTargetPersonExternalLinks( sesTarget )

	iRemovePersonExternalLink = 1
	for tgtMissingPersonExternalLink in tgtMissingPersonExternalLinks:
		removePersonExternalLink = personExternalLinkProcessing.softDeletePersonExternalLink( tgtMissingPersonExternalLink, srcPeople )
		if removePersonExternalLink:
			sesTarget.add( removePersonExternalLink )
			if iRemovePersonExternalLink % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemovePersonExternalLink += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	#	End of personExternalLinkProcessing
	###############################################################################

	resetUpdatedFlag( sesTarget, "people" )
	resetUpdatedFlag( sesTarget, "person_externallinks" )
	resetUpdatedFlag( sesTarget, "person_webprofile" )

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

# End of the process for the source person data (1 table into 3 tables)
###############################################################################


###############################################################################
# Load the asu data warehouse people data table in into the final destination:
#	mysql:
#		bio_public.person_phones to bio_public.person_phones
# 	
# Using Group By on the source to limit likely duplicates.

import personPhoneProcessing

if True:
	resetUpdatedFlag( sesTarget, "person_phones" )

	srcPersonPhones = personPhoneProcessing.getSourcePhones( sesSource )

	iPersonPhone = 1
	for srcPersonPhone in srcPersonPhones:
		processedPhone = personPhoneProcessing.processPhone( srcPersonPhone, sesTarget )
		sesTarget.add( processedPhone )
		if iPersonPhone % 1000 == 0:
			try:
				sesTarget.flush()
			except sqlalchemy.exc.IntegrityError as e:
				sesTarget.rollback()
				raise e
		iPersonPhone += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingPersonPhones = personPhoneProcessing.getTargetPhones( sesTarget )

	iRemovePhone = 1
	for tgtMissingPersonPhone in tgtMissingPersonPhones:
		removePhone = personPhoneProcessing.cleanupSourcePhones( tgtMissingPersonPhone, srcPersonPhones )
		sesTarget.add( removePhone )
		if iRemovePhone % 1000 == 0:
			try:
				sesTarget.flush()
			except sqlalchemy.exc.IntegrityError as e:
				sesTarget.rollback()
				raise e
		iRemovePhone += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	resetUpdatedFlag( sesTarget, "person_phones" )

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of personPhoneProcessing
###############################################################################

###############################################################################
# Load the asu data warehouse people data table in into the final destination:
#	mysql:
#		bio_public.person_addresses to bio_public.person_addresses

import personAddressProcessing

if True:
	resetUpdatedFlag( sesTarget, "person_addresses" )
	srcPersonAddresses = personAddressProcessing.getSourceAddresses( sesSource )

	iPersonAddresses = 1
	for srcPersonAddress in srcPersonAddresses:
		processedAddress = personAddressProcessing.processAddress( srcPersonAddress, sesTarget )
		sesTarget.add( processedAddress )
		if iPersonAddresses % 1000 == 0:
			try:
				sesTarget.flush()
			except sqlalchemy.exc.IntegrityError as e:
				sesTarget.rollback()
				raise e
		iPersonAddresses += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	# REMOVE the data no longer found in the source database...
	tgtMissingPersonAddresses = personAddressProcessing.getTargetAddresses( sesTarget )

	iRemoveAddresses = 1
	for tgtMissingPersonAddress in tgtMissingPersonAddresses:
		removePersonAddress = personAddressProcessing.cleanupSourceAddresses( tgtMissingPersonAddress, srcPersonAddresses )
		if removePersonAddress:
			sesTarget.add( removePersonAddress )
			if iRemoveAddresses % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveAddresses += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	resetUpdatedFlag( sesTarget, "person_addresses" )

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of personAddressProcessing
###############################################################################



###############################################################################
# 
#	File Import: departmentProcessing
# 	pull over all of the data warehouse department codes.

import departmentProcessing

if True:
	srcDepartments = departmentProcessing.getSourceDepartments( sesSource )

	iDepartment = 1
	for srcDepartment in srcDepartments:
		processedDepartment = departmentProcessing.processDepartment( srcDepartment, sesTarget )
		if processedDepartment:
			sesTarget.add( processedDepartment )
			if iDepartment % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iDepartment += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingDepartments = departmentProcessing.getTargetDepartments( sesTarget )

	iRemoveDepartment = 1
	for tgtMissingDepartment in tgtMissingDepartments:
		removeDepartment = departmentProcessing.softDeleteDepartment( tgtMissingDepartment, srcDepartments )
		if removeDepartment:
			sesTarget.add( removeDepartment )
			if iRemoveDepartment % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveDepartment += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

# end of for tgtMissingDepartments
###############################################################################

###############################################################################
# 
#   File Import:  jobProcessing
# 	pull over all of the data warehouse job codes.

import jobProcessing

if True:
	srcJobCodes = jobProcessing.getSourceJobCodes( sesSource )

	iJob = 1
	for srcJob in srcJobCodes:
		processedjob = jobProcessing.processJob( srcJob, sesTarget )
		if processedjob:
			sesTarget.add( processedjob )
			if iJob % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iJob += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingJobCodes = jobProcessing.getTargetJobCodes( sesTarget )

	iRemoveJob = 1
	for tgtMissingJob in tgtMissingJobCodes:
		removeJob = jobProcessing.softDeleteJob( tgtMissingJob, srcJobCodes )
		if removeJob:
			sesTarget.add( removeJob )
			if iRemoveJob % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveJob += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of jobProcessing
###############################################################################

###############################################################################
# 
#   File Import:  jobLogProcessing

import jobLogProcessing

if True:
	srcJobsLog = jobLogProcessing.getSourceJobsLog( sesSource )

	iJobLog = 1
	for srcJobLog in srcJobsLog:
		processedJobLog = jobLogProcessing.processJobLog( srcJobLog, sesTarget )
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


###############################################################################
# 
#	File Import: personJobsProcessing.py
#	The person_jobs data, is a many to many requiring an updated_flag, need it
# 	this to be set to false prior to the data pull...  will dub our data otherwise.
#
import personJobsProcessing

if True:
	resetUpdatedFlag( sesTarget, "person_jobs" )
	srcPersonJobs = personJobsProcessing.getSourcePersonJobs( sesSource )

	iPersonJob = 1
	for srcPersonJob in srcPersonJobs:
		processedPersonJob = personJobsProcessing.processPersonJob( srcPersonJob, sesTarget )
		if processedPersonJob:
			sesTarget.add( processedPersonJob )
			if iPersonJob % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iPersonJob += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingPersonJobs = personJobsProcessing.getTargetPersonJobs( sesTarget )

	iRemovePersonJob = 1
	for tgtMissingPersonJob in tgtMissingPersonJobs:
		removePersonJob = personJobsProcessing.softDeletePersonJob( tgtMissingPersonJob, srcPersonJobs )
		if removePersonJob:
			sesTarget.add( removePersonJob )
			if iRemovePersonJob % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemovePersonJob += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	resetUpdatedFlag( sesTarget, "person_jobs" )

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

# end of for tgtMissingPersonJobs
###############################################################################
	
###############################################################################
# 
#   File Import:  personSubAffiliationProcessing.py
#	The person_department_subaffiliations data, is a many to many requiring an 
#	updated_flag, need it this to be set to false prior to the data pull...  
#	will dub our data otherwise.
#

import personSubAffiliationProcessing

if True:
	resetUpdatedFlag( sesTarget, "person_department_subaffiliations" )
	srcPersonSubAffiliations = personSubAffiliationProcessing.getSourcePersonSubAffiliations( sesSource )

	iPersonSubAffiliation = 1
	for srcPersonSubAffiliation in srcPersonSubAffiliations:
		processedpersonSubAffiliation = personSubAffiliationProcessing.processPersonSubAffiliation( srcPersonSubAffiliation, sesTarget )
		if processedpersonSubAffiliation:
			sesTarget.add( processedpersonSubAffiliation )
			if iPersonSubAffiliation % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iPersonSubAffiliation += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingPersonSubAffiliations = personSubAffiliationProcessing.getTargetPersonSubAffiliations( sesTarget )

	iRemovePersonSubAffiliation = 1
	for tgtMissingPersonSubAffiliation in tgtMissingPersonSubAffiliations:
		removePersonSubAffiliation = personSubAffiliationProcessing.softDeletePersonSubAffiliation( tgtMissingPersonSubAffiliation, srcPersonSubAffiliations )
		if removePersonSubAffiliation:
			sesTarget.add( removePersonSubAffiliation )
			if iRemovePersonSubAffiliation % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemovePersonSubAffiliation += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	resetUpdatedFlag( sesTarget, "person_department_subaffiliations" )
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e


#	End of personSubAffiliationProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farEvaluationProcessing.py

import farEvaluationProcessing

if True:	
	srcFarEvaluations = farEvaluationProcessing.getSourceFarEvaluations( sesSource )

	iFarEvaluation = 1
	for srcFarEvaluation in srcFarEvaluations:
		processedfarEvaluation = farEvaluationProcessing.processFarEvaluation( srcFarEvaluation, sesTarget )
		if processedfarEvaluation:
			sesTarget.add( processedfarEvaluation )
			if iFarEvaluation % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarEvaluation += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarEvaluations = farEvaluationProcessing.getTargetFarEvaluations( sesTarget )

	iRemoveFarEvaluation = 1
	for tgtMissingFarEvaluation in tgtMissingFarEvaluations:
		removeFarEvaluation = farEvaluationProcessing.softDeleteFarEvaluation( tgtMissingFarEvaluation, srcFarEvaluations )
		if removeFarEvaluation:
			sesTarget.add( removeFarEvaluation )
			if iRemoveFarEvaluation % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarEvaluation += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farEvaluationProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farConferenceProceedingProcessing

import farConferenceProceedingProcessing

if True:
	srcFarConferenceProceedings = farConferenceProceedingProcessing.getSourceFarConferenceProceedings( sesSource )

	iFarConferenceProceeding = 1
	for srcFarConferenceProceeding in srcFarConferenceProceedings:
		processedfarConferenceProceeding = farConferenceProceedingProcessing.processFarConferenceProceeding( srcFarConferenceProceeding, sesTarget )
		if processedfarConferenceProceeding:
			sesTarget.add( processedfarConferenceProceeding )
			if iFarConferenceProceeding % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarConferenceProceeding += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarConferenceProceedings = farConferenceProceedingProcessing.getTargetFarConferenceProceedings( sesTarget )

	iRemoveFarConferenceProceeding = 1
	for tgtMissingFarConferenceProceeding in tgtMissingFarConferenceProceedings:
		removeFarConferenceProceeding = farConferenceProceedingProcessing.softDeleteFarConferenceProceeding( tgtMissingFarConferenceProceeding, srcFarConferenceProceedings )
		if removeFarConferenceProceeding:
			sesTarget.add( removeFarConferenceProceeding )
			if iRemoveFarConferenceProceeding % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarConferenceProceeding += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farConferenceProceedingProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farAuthoredBookProcessing

import farAuthoredBookProcessing

if True:
	srcFarAuthoredBooks = farAuthoredBookProcessing.getSourceFarAuthoredBooks( sesSource )

	iFarAuthoredBook = 1
	for srcFarAuthoredBook in srcFarAuthoredBooks:
		processedfarAuthoredBook = farAuthoredBookProcessing.processFarAuthoredBook( srcFarAuthoredBook, sesTarget )
		if processedfarAuthoredBook:
			sesTarget.add( processedfarAuthoredBook )
			if iFarAuthoredBook % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarAuthoredBook += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarAuthoredBooks = farAuthoredBookProcessing.getTargetFarAuthoredBooks( sesTarget )

	iRemoveFarAuthoredBook = 1
	for tgtMissingFarAuthoredBook in tgtMissingFarAuthoredBooks:
		removeFarAuthoredBook = farAuthoredBookProcessing.softDeleteFarAuthoredBook( tgtMissingFarAuthoredBook, srcFarAuthoredBooks )
		if removeFarAuthoredBook:
			sesTarget.add( removeFarAuthoredBook )
			if iRemoveFarAuthoredBook % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarAuthoredBook += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farAuthoredBookProcessing
# ###############################################################################

###############################################################################
# 
#   File Import:  farRefereedarticleProcessing

import farRefereedarticleProcessing

if True:
	srcFarRefereedarticles = farRefereedarticleProcessing.getSourceFarRefereedarticles( sesSource )

	iFarRefereedarticle = 1
	for srcFarRefereedarticle in srcFarRefereedarticles:
		processedfarRefereedarticle = farRefereedarticleProcessing.processFarRefereedarticle( srcFarRefereedarticle, sesTarget )
		if processedfarRefereedarticle:
			sesTarget.add( processedfarRefereedarticle )
			if iFarRefereedarticle % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarRefereedarticle += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarRefereedarticles = farRefereedarticleProcessing.getTargetFarRefereedarticles( sesTarget )

	iRemoveFarRefereedarticle = 1
	for tgtMissingFarRefereedarticle in tgtMissingFarRefereedarticles:
		removeFarRefereedarticle = farRefereedarticleProcessing.softDeleteFarRefereedarticle( tgtMissingFarRefereedarticle, srcFarRefereedarticles )
		if removeFarRefereedarticle:
			sesTarget.add( removeFarRefereedarticle )
			if iRemoveFarRefereedarticle % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarRefereedarticle += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farRefereedarticleProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farNonrefereedarticleProcessing

import farNonrefereedarticleProcessing

if True:
	srcFarNonrefereedarticles = farNonrefereedarticleProcessing.getSourceFarNonrefereedarticles( sesSource )

	iFarNonrefereedarticle = 1
	for srcFarNonrefereedarticle in srcFarNonrefereedarticles:
		processedfarNonrefereedarticle = farNonrefereedarticleProcessing.processFarNonrefereedarticle( srcFarNonrefereedarticle, sesTarget )
		if processedfarNonrefereedarticle:
			sesTarget.add( processedfarNonrefereedarticle )
			if iFarNonrefereedarticle % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarNonrefereedarticle += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarNonrefereedarticles = farNonrefereedarticleProcessing.getTargetFarNonrefereedarticles( sesTarget )

	iRemoveFarNonrefereedarticle = 1
	for tgtMissingFarNonrefereedarticle in tgtMissingFarNonrefereedarticles:
		removeFarNonrefereedarticle = farNonrefereedarticleProcessing.softDeleteFarNonrefereedarticle( tgtMissingFarNonrefereedarticle, srcFarNonrefereedarticles )
		if removeFarNonrefereedarticle:
			sesTarget.add( removeFarNonrefereedarticle )
			if iRemoveFarNonrefereedarticle % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarNonrefereedarticle += 1

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farNonrefereedarticleProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farEditedbookProcessing

import farEditedbookProcessing

if True:
	srcFarEditedbooks = farEditedbookProcessing.getSourceFarEditedbooks( sesSource )

	iFarEditedbook = 1
	for srcFarEditedbook in srcFarEditedbooks:
		processedfarEditedbook = farEditedbookProcessing.processFarEditedbook( srcFarEditedbook, sesTarget )
		if processedfarEditedbook:
			sesTarget.add( processedfarEditedbook )
			if iFarEditedbook % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarEditedbook += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarEditedbooks = farEditedbookProcessing.getTargetFarEditedbooks( sesTarget )

	iRemoveFarEditedbook = 1
	for tgtMissingFarEditedbook in tgtMissingFarEditedbooks:
		removeFarEditedbook = farEditedbookProcessing.softDeleteFarEditedbook( tgtMissingFarEditedbook, srcFarEditedbooks )
		if removeFarEditedbook:
			sesTarget.add( removeFarEditedbook )
			if iRemoveFarEditedbook % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarEditedbook += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farEditedbookProcessing
###############################################################################


###############################################################################
# 
#   File Import:  farBookChapterProcessing

import farBookChapterProcessing

if True:
	srcFarBookChapters = farBookChapterProcessing.getSourceFarBookChapters( sesSource )

	iFarBookChapter = 1
	for srcFarBookChapter in srcFarBookChapters:
		processedfarBookChapter = farBookChapterProcessing.processFarBookChapter( srcFarBookChapter, sesTarget )
		if processedfarBookChapter:
			sesTarget.add( processedfarBookChapter )
			if iFarBookChapter % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarBookChapter += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarBookChapters = farBookChapterProcessing.getTargetFarBookChapters( sesTarget )

	iRemoveFarBookChapter = 1
	for tgtMissingFarBookChapter in tgtMissingFarBookChapters:
		removeFarBookChapter = farBookChapterProcessing.softDeleteFarBookChapter( tgtMissingFarBookChapter, srcFarBookChapters )
		if removeFarBookChapter:
			sesTarget.add( removeFarBookChapter )
			if iRemoveFarBookChapter % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarBookChapter += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farBookChapterProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farBookReviewProcessing

import farBookReviewProcessing

if True:
	srcFarBookReviews = farBookReviewProcessing.getSourceFarBookReviews( sesSource )

	iFarBookReview = 1
	for srcFarBookReview in srcFarBookReviews:
		processedFarBookReview = farBookReviewProcessing.processFarBookReview( srcFarBookReview, sesTarget )
		if processedFarBookReview:
			sesTarget.add( processedFarBookReview )
			if iFarBookReview % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarBookReview += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarBookReviews = farBookReviewProcessing.getTargetFarBookReviews( sesTarget )

	iRemoveFarBookReview = 1
	for tgtMissingFarBookReview in tgtMissingFarBookReviews:
		removeFarBookReview = farBookReviewProcessing.softDeleteFarBookReview( tgtMissingFarBookReview, srcFarBookReviews )
		if removeFarBookReview:
			sesTarget.add( removeFarBookReview )
			if iRemoveFarBookReview % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarBookReview += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farBookReviewProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farEncyclopediaarticleProcessing

import farEncyclopediaarticleProcessing

if True:
	srcFarEncyclopediaarticles = farEncyclopediaarticleProcessing.getSourceFarEncyclopediaarticles( sesSource )

	iFarEncyclopediaarticle = 1
	for srcFarEncyclopediaarticle in srcFarEncyclopediaarticles:
		processedFarEncyclopediaarticle = farEncyclopediaarticleProcessing.processFarEncyclopediaarticle( srcFarEncyclopediaarticle, sesTarget )
		if processedFarEncyclopediaarticle:
			sesTarget.add( processedFarEncyclopediaarticle )
			if iFarEncyclopediaarticle % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarEncyclopediaarticle += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarEncyclopediaarticles = farEncyclopediaarticleProcessing.getTargetFarEncyclopediaarticles( sesTarget )

	iRemoveFarEncyclopediaarticle = 1
	for tgtMissingFarEncyclopediaarticle in tgtMissingFarEncyclopediaarticles:
		removeFarEncyclopediaarticle = farEncyclopediaarticleProcessing.softDeleteFarEncyclopediaarticle( tgtMissingFarEncyclopediaarticle, srcFarEncyclopediaarticles )
		if removeFarEncyclopediaarticle:
			sesTarget.add( removeFarEncyclopediaarticle )
			if iRemoveFarEncyclopediaarticle % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarEncyclopediaarticle += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farEncyclopediaarticleProcessing
###############################################################################

###############################################################################
# 
#   File Import:  farShortstoriesProcessing

import farShortstoriesProcessing

if True:
	srcFarShortstories = farShortstoriesProcessing.getSourceFarShortstories( sesSource )

	iFarShortstorie = 1
	for srcFarShortstorie in srcFarShortstories:
		processedFarShortstories = farShortstoriesProcessing.processFarShortstorie( srcFarShortstorie, sesTarget )
		if processedFarShortstories:
			sesTarget.add( processedFarShortstories )
			if iFarShortstorie % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iFarShortstorie += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	tgtMissingFarShortstories = farShortstoriesProcessing.getTargetFarShortstories( sesTarget )

	iRemoveFarShortstorie = 1
	for tgtMissingFarShortstorie in tgtMissingFarShortstories:
		removeFarShortstorie = farShortstoriesProcessing.softDeleteFarShortstorie( tgtMissingFarShortstorie, srcFarShortstories )
		if removeFarShortstorie:
			sesTarget.add( removeFarShortstorie )
			if iRemoveFarShortstorie % 1000 == 0:
				try:
					sesTarget.flush()
				except sqlalchemy.exc.IntegrityError as e:
					sesTarget.rollback()
					raise e
			iRemoveFarShortstorie += 1
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

#	End of farShortstoriesProcessing
###############################################################################

cleanUp( None )

#
# 	End of the BIOETL scripts...
###############################################################################
