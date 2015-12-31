import datetime

from sharedProcesses import resetUpdatedFlag
from app.connectdb import EtlConnections

bioetlAppRun = EtlConnections("asutobio")

try:
	sesSource = bioetlAppRun.getSourceSession()
	sesTarget = bioetlAppRun.getTargetSession()

	###############################################################################
	### biopublicLoad.py cut and paste into proper stop in the file all below:
	###############################################################################


	###############################################################################
	# 	The Biodesign Defined subaffiliations.  There is BI aspects of this data
	#		that warrents that it be scoped.  Reference table so ok to load prior
	#		to the people data, and must be loaded prior to the 
	#   File Import:  subAffiliationProcessing

	try:
		resetUpdatedFlag( sesTarget, "subaffiliations" )
	except Exception as e:
		print e

	import subAffiliationProcessing

	srcSubAffiliations = subAffiliationProcessing.getSourceSubAffiliations( )

	iSubAffiliation = 1
	for srcSubAffiliation in srcSubAffiliations:
		try:
			processedsubAffiliation = subAffiliationProcessing.processSubAffiliation( srcSubAffiliation, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeSubAffiliation = subAffiliationProcessing.softDeleteSubAffiliation( tgtMissingSubAffiliation, srcSubAffiliations )
		except TypeError as e:
			pass
		else:
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

	try:
		resetUpdatedFlag( sesTarget, "subaffiliations" )
	except Exception as e:
		print e

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
	try:
		resetUpdatedFlag( sesTarget, "people" )
		resetUpdatedFlag( sesTarget, "person_externallinks" )
		resetUpdatedFlag( sesTarget, "person_webprofile" )
	except Exception as e:
		print e

	import personProcessing
	import personWebProfileProcessing
	import personExternalLinkProcessing

	srcPeople = personProcessing.getSourcePeople( sesSource )

	iPerson = 1
	for srcPerson in srcPeople:
		try:
			personStatus = personProcessing.processPerson( srcPerson, sesTarget )
		except TypeError as e:
			pass  # replace this with 'continue'
		else:
			sesTarget.add( personStatus )

			try:
				processedpersonWebProfile = personWebProfileProcessing.processPersonWebProfile( srcPerson, sesTarget )
			except TypeError as e:
				pass
			else:
				if processedpersonWebProfile:
					sesTarget.add( processedpersonWebProfile )

			try:
				processedpersonExternalLink = personExternalLinkProcessing.processPersonExternalLink( srcPerson, sesTarget )
			except TypeError as e:
				pass
			else:
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
		try:
			personMissing = personProcessing.softDeletePerson( tgtMissingPerson, srcPeople )
		except TypeError as e:
			# print e
			pass
		else:
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
		try:
			removePersonWebProfile = personWebProfileProcessing.softDeletePersonWebProfile( tgtMissingPersonWebProfile, srcPeople )
		except TypeError as e:
			pass
		else:
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
		try:
			removePersonExternalLink = personExternalLinkProcessing.softDeletePersonExternalLink( tgtMissingPersonExternalLink, srcPeople )
		except TypeError as e:
			pass
		else:
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

	try:
		resetUpdatedFlag( sesTarget, "people" )
		resetUpdatedFlag( sesTarget, "person_externallinks" )
		resetUpdatedFlag( sesTarget, "person_webprofile" )
	except Exception as e:
		print e

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
	#
	try:
		resetUpdatedFlag( sesTarget, "person_phones" )
	except Exception as e:
		print e

	import personPhoneProcessing

	srcPersonPhones = personPhoneProcessing.getSourcePhones( sesSource )

	iPersonPhone = 1
	for srcPersonPhone in srcPersonPhones:
		try:
			processedPhone = personPhoneProcessing.processPhone( srcPersonPhone, sesTarget )
		except TypeError as e:
			pass
		else:
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

	# Remove the data that was no longer was found for an active person.
	tgtMissingPersonPhones = personPhoneProcessing.getTargetPhones( sesTarget )

	iRemovePhone = 1
	for tgtMissingPersonPhone in tgtMissingPersonPhones:
		# if the phone no longer is found we remove it but only if the person is active...
		try:
			removePhone = personPhoneProcessing.cleanupSourcePhones( tgtMissingPersonPhone, srcPersonPhones )
		except TypeError as e:
			pass
		else:
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

	# reset the updated_flag for all records for the next round of changes.
	# engineTarget.execute("UPDATE person_phones SET updated_flag = 0;")
	try:
		resetUpdatedFlag( sesTarget, "person_phones" )
	except Exception as e:
		print e

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e


	#	End of personPhoneProcessing
	###############################################################################



####**####**####**####**####**####**####**####**####**####**####**####
# CODE UPDATE Stopped HERE...  BELOW NEEDS UPDATES...
####**####**####**####**####**####**####**####**####**####**####**####

	###############################################################################
	# Load the asu data warehouse people data table in into the final destination:
	#	mysql:
	#		bio_public.person_addresses to bio_public.person_addresses
	# 	
	# Using Group By on the source to limit likely duplicates.
	try:
		resetUpdatedFlag( sesTarget, "person_addresses" )
	except Exception as e:
		print e

	import personAddressProcessing

	srcPersonAddresses = personAddressProcessing.getSourceAddresses( sesSource )

	iPersonAddresses = 1
	for srcPersonAddress in srcPersonAddresses:
		try:
			processedAddress = personAddressProcessing.processAddress( srcPersonAddress, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removePersonAddress = personAddressProcessing.cleanupSourceAddresses( tgtMissingPersonAddress, srcPersonAddresses )
		except TypeError as e:
			pass
		else:
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

	# RESET the updated_flag for all records for the next round of changes.
	try:
		resetUpdatedFlag( sesTarget, "person_addresses" )
	except Exception as e:
		print e

	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	#	End of personAddressProcessing
	###############################################################################



	# ###############################################################################
	# 
	# pull over all of the data warehouse department codes.

	import departmentProcessing

	srcDepartments = departmentProcessing.getSourceDepartments( sesSource )

	iDepartment = 1
	for srcDepartment in srcDepartments:
		try:
			processedDepartment = departmentProcessing.processDepartment( srcDepartment, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeDepartment = departmentProcessing.softDeleteDepartment( tgtMissingDepartment, srcDepartments )
		except TypeError as e:
			pass
		else:
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
	# pull over all of the data warehouse job codes.

	import jobProcessing

	srcJobCodes = jobProcessing.getSourceJobCodes( sesSource )

	iJob = 1
	for srcJob in srcJobCodes:
		try:
			processedjob = jobProcessing.processJob( srcJob, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeJob = jobProcessing.softDeleteJob( tgtMissingJob, srcJobCodes )
		except TypeError as e:
			pass
		else:
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

	srcJobsLog = jobLogProcessing.getSourceJobsLog( sesSource )

	iJobLog = 1
	for srcJobLog in srcJobsLog:
		try:
			jrocessedJobLog = jobLogProcessing.processJobLog( srcJobLog, sesTarget )
		except TypeError as e:
			pass
		else:
			sesTarget.add( jrocessedJobLog )
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
		try:
			removeJobLog = jobLogProcessing.softDeleteJobLog( tgtMissingJobLog, srcJobsLog )
		except TypeError as e:
			pass
		else:
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
	try:
		resetUpdatedFlag( sesTarget, "person_jobs" )
	except Exception as e:
		print e
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e
	import personJobsProcessing

	srcPersonJobs = personJobsProcessing.getSourcePersonJobs( sesSource )

	iPersonJob = 1
	for srcPersonJob in srcPersonJobs:
		try:
			processedPersonJob = personJobsProcessing.processPersonJob( srcPersonJob, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removePersonJob = personJobsProcessing.softDeletePersonJob( tgtMissingPersonJob, srcPersonJobs )
		except TypeError as e:
			pass
		else:
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

	try:
		resetUpdatedFlag( sesTarget, "person_jobs" )
	except Exception as e:
		print e
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
	try:
		resetUpdatedFlag( sesTarget, "person_department_subaffiliations" )
	except Exception as e:
		print e
	try:
		sesTarget.commit()
	except sqlalchemy.exc.IntegrityError as e:
		sesTarget.rollback()
		raise e

	import personSubAffiliationProcessing

	srcPersonSubAffiliations = personSubAffiliationProcessing.getSourcePersonSubAffiliations( sesSource )

	iPersonSubAffiliation = 1
	for srcPersonSubAffiliation in srcPersonSubAffiliations:
		try:
			processedpersonSubAffiliation = personSubAffiliationProcessing.processPersonSubAffiliation( srcPersonSubAffiliation, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removePersonSubAffiliation = personSubAffiliationProcessing.softDeletePersonSubAffiliation( tgtMissingPersonSubAffiliation, srcPersonSubAffiliations )
		except TypeError as e:
			pass
		# except RuntimeError as e:
		# 	raise e
		else:
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

	try:
		resetUpdatedFlag( sesTarget, "person_department_subaffiliations" )
	except Exception as e:
		print e
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

	srcFarEvaluations = farEvaluationProcessing.getSourceFarEvaluations( sesSource )

	iFarEvaluation = 1
	for srcFarEvaluation in srcFarEvaluations:
		try:
			processedfarEvaluation = farEvaluationProcessing.processFarEvaluation( srcFarEvaluation, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarEvaluation = farEvaluationProcessing.softDeleteFarEvaluation( tgtMissingFarEvaluation, srcFarEvaluations )
		except TypeError as e:
			pass
		else:
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

	srcFarConferenceProceedings = farConferenceProceedingProcessing.getSourceFarConferenceProceedings( sesSource )

	iFarConferenceProceeding = 1
	for srcFarConferenceProceeding in srcFarConferenceProceedings:
		try:
			processedfarConferenceProceeding = farConferenceProceedingProcessing.processFarConferenceProceeding( srcFarConferenceProceeding, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarConferenceProceeding = farConferenceProceedingProcessing.softDeleteFarConferenceProceeding( tgtMissingFarConferenceProceeding, srcFarConferenceProceedings )
		except TypeError as e:
			pass
		else:
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

	srcFarAuthoredBooks = farAuthoredBookProcessing.getSourceFarAuthoredBooks( sesSource )

	iFarAuthoredBook = 1
	for srcFarAuthoredBook in srcFarAuthoredBooks:
		try:
			processedfarAuthoredBook = farAuthoredBookProcessing.processFarAuthoredBook( srcFarAuthoredBook, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarAuthoredBook = farAuthoredBookProcessing.softDeleteFarAuthoredBook( tgtMissingFarAuthoredBook, srcFarAuthoredBooks )
		except TypeError as e:
			pass
		else:
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

	srcFarRefereedarticles = farRefereedarticleProcessing.getSourceFarRefereedarticles( sesSource )

	iFarRefereedarticle = 1
	for srcFarRefereedarticle in srcFarRefereedarticles:
		try:
			processedfarRefereedarticle = farRefereedarticleProcessing.processFarRefereedarticle( srcFarRefereedarticle, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarRefereedarticle = farRefereedarticleProcessing.softDeleteFarRefereedarticle( tgtMissingFarRefereedarticle, srcFarRefereedarticles )
		except TypeError as e:
			pass
		else:
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

	srcFarNonrefereedarticles = farNonrefereedarticleProcessing.getSourceFarNonrefereedarticles( sesSource )

	iFarNonrefereedarticle = 1
	for srcFarNonrefereedarticle in srcFarNonrefereedarticles:
		try:
			processedfarNonrefereedarticle = farNonrefereedarticleProcessing.processFarNonrefereedarticle( srcFarNonrefereedarticle, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarNonrefereedarticle = farNonrefereedarticleProcessing.softDeleteFarNonrefereedarticle( tgtMissingFarNonrefereedarticle, srcFarNonrefereedarticles )
		except TypeError as e:
			pass
		else:
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

	srcFarEditedbooks = farEditedbookProcessing.getSourceFarEditedbooks( sesSource )

	iFarEditedbook = 1
	for srcFarEditedbook in srcFarEditedbooks:
		try:
			processedfarEditedbook = farEditedbookProcessing.processFarEditedbook( srcFarEditedbook, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarEditedbook = farEditedbookProcessing.softDeleteFarEditedbook( tgtMissingFarEditedbook, srcFarEditedbooks )
		except TypeError as e:
			pass
		else:
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

	srcFarBookChapters = farBookChapterProcessing.getSourceFarBookChapters( sesSource )

	iFarBookChapter = 1
	for srcFarBookChapter in srcFarBookChapters:
		try:
			processedfarBookChapter = farBookChapterProcessing.processFarBookChapter( srcFarBookChapter, sesTarget )
		except TypeError as e:
			pass
		else:
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
		try:
			removeFarBookChapter = farBookChapterProcessing.softDeleteFarBookChapter( tgtMissingFarBookChapter, srcFarBookChapters )
		except TypeError as e:
			pass
		else:
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

	srcFarBookReviews = farBookReviewProcessing.getSourceFarBookReviews( sesSource )

	iFarBookReview = 1
	for srcFarBookReview in srcFarBookReviews:
		try:
			frocessedFarBookReview = farBookReviewProcessing.processFarBookReview( srcFarBookReview, sesTarget )
		except TypeError as e:
			pass
		else:
			sesTarget.add( frocessedFarBookReview )
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
		try:
			removeFarBookReview = farBookReviewProcessing.softDeleteFarBookReview( tgtMissingFarBookReview, srcFarBookReviews )
		except TypeError as e:
			pass
		else:
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

	srcFarEncyclopediaarticles = farEncyclopediaarticleProcessing.getSourceFarEncyclopediaarticles( sesSource )

	iFarEncyclopediaarticle = 1
	for srcFarEncyclopediaarticle in srcFarEncyclopediaarticles:
		try:
			frocessedFarEncyclopediaarticle = farEncyclopediaarticleProcessing.processFarEncyclopediaarticle( srcFarEncyclopediaarticle, sesTarget )
		except TypeError as e:
			pass
		else:
			sesTarget.add( frocessedFarEncyclopediaarticle )
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
		try:
			removeFarEncyclopediaarticle = farEncyclopediaarticleProcessing.softDeleteFarEncyclopediaarticle( tgtMissingFarEncyclopediaarticle, srcFarEncyclopediaarticles )
		except TypeError as e:
			pass
		else:
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

	srcFarShortstories = farShortstoriesProcessing.getSourceFarShortstories( sesSource )

	iFarShortstorie = 1
	for srcFarShortstorie in srcFarShortstories:
		try:
			frocessedFarShortstories = farShortstoriesProcessing.processFarShortstorie( srcFarShortstorie, sesTarget )
		except TypeError as e:
			pass
		else:
			sesTarget.add( frocessedFarShortstories )
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
		try:
			removeFarShortstorie = farShortstoriesProcessing.softDeleteFarShortstorie( tgtMissingFarShortstorie, srcFarShortstories )
		except TypeError as e:
			pass
		else:
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


except IndexError as e:
	raise e
finally:
	sesTarget.close()
	sesSource.close()

	bioetlAppRun.cleanUp()
#
# End the processing of person addresses records
###############################################################################
