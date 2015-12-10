import datetime

import sharedProcesses

from app.connectdb import EtlConnections

bioetlAppRun = EtlConnections("asutobio")

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

# import subAffiliationProcessing

# srcSubAffiliations = subAffiliationProcessing.getSourceSubAffiliations( sesSource )

# iSubAffiliation = 1
# for srcSubAffiliation in srcSubAffiliations:
# 	try:
# 		processedsubAffiliation = subAffiliationProcessing.processSubAffiliation( srcSubAffiliation, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedsubAffiliation )
# 		if iSubAffiliation % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iSubAffiliation += 1
# try:
# 	sesTarget.commit()
# # except Exception as e:
# #possible replacement to the very generic Exception...
# except sqlalchemy.exc.IntegrityError:
# 	sesTarget.rollback()
# 	# raise e


# tgtMissingSubAffiliations = subAffiliationProcessing.getTargetSubAffiliations( sesTarget )

# iRemoveSubAffiliation = 1
# for tgtMissingSubAffiliation in tgtMissingSubAffiliations:
# 	try:
# 		removeSubAffiliation = subAffiliationProcessing.softDeleteSubAffiliation( tgtMissingSubAffiliation, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeSubAffiliation )
# 		if iRemoveSubAffiliation % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveSubAffiliation += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of subAffiliationProcessing
# ###############################################################################


# ###############################################################################
# # Load the mysql.bio_ps people table in into the final destination:
# #	mysql:
# #		bio_ps.people to bio_public.people
# # 
# import personProcessing

# srcPeople = personProcessing.getSourcePeople( sesSource )

# iPerson = 1
# for srcPerson in srcPeople:
# 	try:
# 		personStatus = personProcessing.processPerson( srcPerson, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( personStatus )
# 		if iPerson % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 			except RuntimeError as e:
# 				raise e
# 		iPerson += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # 
# # "REMOVE" with a soft delete of person records no longer found in the source database
# tgtMissingPeople = personProcessing.getTargetPeople( sesTarget )

# iMissingPerson = 1
# for tgtMissingPerson in tgtMissingPeople:
# 	try:
# 		personMissing = personProcessing.softDeletePerson( tgtMissingPerson, sesSource )
# 	except TypeError as e:
# 		# print e
# 		pass
# 	else:
# 		sesTarget.add( personMissing )
# 		if iMissingPerson % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iMissingPerson += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e
# # End of the process for the person data 
# ###############################################################################


# ###############################################################################
# # Load the mysql.bio_ps people data table in into the final destination:
# #	mysql:
# #		bio_ps.person_phones to bio_public.person_phones
# # 	
# # Using Group By on the source to limit likely duplicates.
# #
# import personPhoneProcessing

# srcPersonPhones = personPhoneProcessing.getSourcePhones( sesSource )

# iPersonPhone = 1
# for srcPersonPhone in srcPersonPhones:
# 	try:
# 		processPhone = personPhoneProcessing.processPhone( srcPersonPhone, sesTarget )
# 	except Exception as e:
# 		# print e
# 		# raise e
# 		pass
# 	else:
# 		sesTarget.add( processPhone )
# 		if iPersonPhone % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iPersonPhone += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # Remove the data that was no longer was found for an active person.
# tgtMissingPersonPhones = personPhoneProcessing.getTargetPhones( sesTarget )

# iRemovePhone = 1
# for tgtMissingPersonPhone in tgtMissingPersonPhones:
# 	# if the phone no longer is found we remove it but only if the person is active...
# 	try:
# 		removePhone = personPhoneProcessing.cleanupSourcePhones( tgtMissingPersonPhone, sesSource )
# 	except TypeError, e:
# 		# print e
# 		pass
# 	else:
# 		sesTarget.add( tgtMissingPersonPhone )
# 		if iRemovePhone % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemovePhone += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # reset the updated_flag for all records for the next round of changes.
# # engineTarget.execute("UPDATE person_phones SET updated_flag = 0;")
# try:
# 	sharedProcesses.resetUpdatedFlag( sesTarget, "person_phones" )
# except Exception as e:
# 	print e

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# ###############################################################################
# # Load the mysql.bio_ps people data table in into the final destination:
# #	mysql:
# #		bio_ps.person_addresses to bio_public.person_addresses
# # 	
# # Using Group By on the source to limit likely duplicates.
# import personAddressProcessing

# srcPersonAddresses = personAddressProcessing.getSourceAddresses( sesSource )

# iPersonAddresses = 1
# for srcPersonAddress in srcPersonAddresses:
# 	try:
# 		processedAddress = personAddressProcessing.processAddress( srcPersonAddress, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedAddress )
# 		if iPersonAddresses % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iPersonAddresses += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # REMOVE the data no longer found in the source database...
# tgtMissingPersonAddresses = personAddressProcessing.getTargetAddresses( sesTarget )

# iRemoveAddresses = 1
# for tgtMissingPersonAddress in tgtMissingPersonAddresses:
# 	try:
# 		removePersonAddress = personAddressProcessing.cleanupSourceAddresses( tgtMissingPersonAddress, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( tgtMissingPersonAddress )
# 		if iRemoveAddresses % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveAddresses += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # RESET the updated_flag for all records for the next round of changes.
# try:
# 	sharedProcesses.resetUpdatedFlag( sesTarget, "person_addresses" )
# except Exception as e:
# 	print e

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# ###############################################################################
# # 
# #   personWebProfileProcessing


# import personWebProfileProcessing

# srcPersonWebProfiles = personWebProfileProcessing.getSourcePersonWebProfile( sesSource )

# iPersonWebProfile = 1
# for srcPersonWebProfile in srcPersonWebProfiles:
# 	try:
# 		processedpersonWebProfile = personWebProfileProcessing.processPersonWebProfile( srcPersonWebProfile, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedpersonWebProfile )
# 		if iPersonWebProfile % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iPersonWebProfile += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingPersonWebProfiles = personWebProfileProcessing.getTargetPersonWebProfiles( sesTarget )

# iRemove_Y_ = 1
# for tgtMissingPersonWebProfile in tgtMissingPersonWebProfiles:
# 	try:
# 		removePersonWebProfile = personWebProfileProcessing.softDeletePersonWebProfile( tgtMissingPersonWebProfile, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removePersonWebProfile )
# 		if iRemove_Y_ % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemove_Y_ += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # 	End of personWebProfileProcessing
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  personExternalLinkProcessing

# import personExternalLinkProcessing

# srcPersonExternalLinks = personExternalLinkProcessing.getSourcePersonExternalLinks( sesSource )

# iPersonExternalLink = 1
# for srcPersonExternalLink in srcPersonExternalLinks:
# 	try:
# 		processedpersonExternalLink = personExternalLinkProcessing.processPersonExternalLink( srcPersonExternalLink, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedpersonExternalLink )
# 		if iPersonExternalLink % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iPersonExternalLink += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingPersonExternalLinks = personExternalLinkProcessing.getTargetPersonExternalLinks( sesTarget )

# iRemovePersonExternalLink = 1
# for tgtMissingPersonExternalLink in tgtMissingPersonExternalLinks:
# 	try:
# 		removePersonExternalLink = personExternalLinkProcessing.softDeletePersonExternalLink( tgtMissingPersonExternalLink, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removePersonExternalLink )
# 		if iRemovePersonExternalLink % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemovePersonExternalLink += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of personExternalLinkProcessing
# ###############################################################################

# ###############################################################################
# # 
# # xxxx

# import departmentProcessing

# srcDepartments = departmentProcessing.getSourceDepartments( sesSource )

# iDepartment = 1
# for srcDepartment in srcDepartments:
# 	try:
# 		processedDepartment = departmentProcessing.processDepartment( srcDepartment, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedDepartment )
# 		if iDepartment % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iDepartment += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingDepartments = departmentProcessing.getTargetDepartments( sesTarget )

# iRemoveDepartment = 1
# for tgtMissingDepartment in tgtMissingDepartments:
# 	try:
# 		removeDepartment = departmentProcessing.softDeleteDepartment( tgtMissingDepartment )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeDepartment )
# 		if iRemoveDepartment % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveDepartment += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # end of for tgtMissingDepartments
# ###############################################################################

# ###############################################################################
# # 
# #
# import personJobsProcessing

# srcPersonJobs = personJobsProcessing.getSourcePersonJobs( sesSource )

# iPersonJob = 1
# for srcPersonJob in srcPersonJobs:
# 	try:
# 		processedPersonJob = personJobsProcessing.processPersonJob( srcPersonJob, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedPersonJob )
# 		if iPersonJob % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iPersonJob += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# tgtMissingPersonJobs = personJobsProcessing.getTargetPersonJobs( sesTarget )

# iRemovePersonJob = 1
# for tgtMissingPersonJob in tgtMissingPersonJobs:
# 	try:
# 		removePersonJob = personJobsProcessing.softDeletePersonJob( tgtMissingPersonJob, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removePersonJob )
# 		if iRemovePersonJob % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemovePersonJob += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # end of for tgtMissingPersonJobs
# ###############################################################################

# ###############################################################################
# ### biopublicLoad.py cut and paste into proper stop in the file all below:
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  personSubAffiliationProcessing

# import personSubAffiliationProcessing

# srcPersonSubAffiliations = personSubAffiliationProcessing.getSourcePersonSubAffiliations( sesSource )

# iPersonSubAffiliation = 1
# for srcPersonSubAffiliation in srcPersonSubAffiliations:
# 	try:
# 		processedpersonSubAffiliation = personSubAffiliationProcessing.processPersonSubAffiliation( srcPersonSubAffiliation, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedpersonSubAffiliation )
# 		if iPersonSubAffiliation % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iPersonSubAffiliation += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingPersonSubAffiliations = personSubAffiliationProcessing.getTargetPersonSubAffiliations( sesTarget )

# iRemovePersonSubAffiliation = 1
# for tgtMissingPersonSubAffiliation in tgtMissingPersonSubAffiliations:
# 	try:
# 		removePersonSubAffiliation = personSubAffiliationProcessing.softDeletePersonSubAffiliation( tgtMissingPersonSubAffiliation, sesSource )
# 	except TypeError as e:
# 		pass
# 	except RuntimeError as e:
# 		raise e
# 	else:
# 		sesTarget.add( removePersonSubAffiliation )
# 		if iRemovePersonSubAffiliation % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemovePersonSubAffiliation += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# # reset the updated_flag for all records for the next round of changes.
# # engineTarget.execute("UPDATE person_phones SET updated_flag = 0;")
# try:
# 	sharedProcesses.resetUpdatedFlag( sesTarget, "person_department_subaffiliations" )
# except Exception as e:
# 	print e


# #	End of personSubAffiliationProcessing
# ###############################################################################



# ###############################################################################
# # 
# #   File Import:  farEvaluationProcessing

# import farEvaluationProcessing

# srcFarEvaluations = farEvaluationProcessing.getSourceFarEvaluations( sesSource )

# iFarEvaluation = 1
# for srcFarEvaluation in srcFarEvaluations:
# 	try:
# 		processedfarEvaluation = farEvaluationProcessing.processFarEvaluation( srcFarEvaluation, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarEvaluation )
# 		if iFarEvaluation % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarEvaluation += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarEvaluations = farEvaluationProcessing.getTargetFarEvaluations( sesTarget )

# iRemoveFarEvaluation = 1
# for tgtMissingFarEvaluation in tgtMissingFarEvaluations:
# 	try:
# 		removeFarEvaluation = farEvaluationProcessing.softDeleteFarEvaluation( tgtMissingFarEvaluation, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarEvaluation )
# 		if iRemoveFarEvaluation % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarEvaluation += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farEvaluationProcessing
# ###############################################################################



# ###############################################################################
# # 
# #   File Import:  farConferenceProceedingProcessing

# import farConferenceProceedingProcessing

# srcFarConferenceProceedings = farConferenceProceedingProcessing.getSourceFarConferenceProceedings( sesSource )

# iFarConferenceProceeding = 1
# for srcFarConferenceProceeding in srcFarConferenceProceedings:
# 	try:
# 		processedfarConferenceProceeding = farConferenceProceedingProcessing.processFarConferenceProceeding( srcFarConferenceProceeding, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarConferenceProceeding )
# 		if iFarConferenceProceeding % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarConferenceProceeding += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarConferenceProceedings = farConferenceProceedingProcessing.getTargetFarConferenceProceedings( sesTarget )

# iRemoveFarConferenceProceeding = 1
# for tgtMissingFarConferenceProceeding in tgtMissingFarConferenceProceedings:
# 	try:
# 		removeFarConferenceProceeding = farConferenceProceedingProcessing.softDeleteFarConferenceProceeding( tgtMissingFarConferenceProceeding, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarConferenceProceeding )
# 		if iRemoveFarConferenceProceeding % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarConferenceProceeding += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farConferenceProceedingProcessing
# ###############################################################################

# ###############################################################################
# # 
# #   File Import:  farAuthoredBookProcessing

# import farAuthoredBookProcessing

# srcFarAuthoredBooks = farAuthoredBookProcessing.getSourceFarAuthoredBooks( sesSource )

# iFarAuthoredBook = 1
# for srcFarAuthoredBook in srcFarAuthoredBooks:
# 	try:
# 		processedfarAuthoredBook = farAuthoredBookProcessing.processFarAuthoredBook( srcFarAuthoredBook, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarAuthoredBook )
# 		if iFarAuthoredBook % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarAuthoredBook += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarAuthoredBooks = farAuthoredBookProcessing.getTargetFarAuthoredBooks( sesTarget )

# iRemoveFarAuthoredBook = 1
# for tgtMissingFarAuthoredBook in tgtMissingFarAuthoredBooks:
# 	try:
# 		removeFarAuthoredBook = farAuthoredBookProcessing.softDeleteFarAuthoredBook( tgtMissingFarAuthoredBook, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarAuthoredBook )
# 		if iRemoveFarAuthoredBook % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarAuthoredBook += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farAuthoredBookProcessing
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  farRefereedarticleProcessing

# import farRefereedarticleProcessing

# srcFarRefereedarticles = farRefereedarticleProcessing.getSourceFarRefereedarticles( sesSource )

# iFarRefereedarticle = 1
# for srcFarRefereedarticle in srcFarRefereedarticles:
# 	try:
# 		processedfarRefereedarticle = farRefereedarticleProcessing.processFarRefereedarticle( srcFarRefereedarticle, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarRefereedarticle )
# 		if iFarRefereedarticle % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarRefereedarticle += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarRefereedarticles = farRefereedarticleProcessing.getTargetFarRefereedarticles( sesTarget )

# iRemoveFarRefereedarticle = 1
# for tgtMissingFarRefereedarticle in tgtMissingFarRefereedarticles:
# 	try:
# 		removeFarRefereedarticle = farRefereedarticleProcessing.softDeleteFarRefereedarticle( tgtMissingFarRefereedarticle, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarRefereedarticle )
# 		if iRemoveFarRefereedarticle % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarRefereedarticle += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farRefereedarticleProcessing
# ###############################################################################



# ###############################################################################
# # 
# #   File Import:  farNonrefereedarticleProcessing

# import farNonrefereedarticleProcessing

# srcFarNonrefereedarticles = farNonrefereedarticleProcessing.getSourceFarNonrefereedarticles( sesSource )

# iFarNonrefereedarticle = 1
# for srcFarNonrefereedarticle in srcFarNonrefereedarticles:
# 	try:
# 		processedfarNonrefereedarticle = farNonrefereedarticleProcessing.processFarNonrefereedarticle( srcFarNonrefereedarticle, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarNonrefereedarticle )
# 		if iFarNonrefereedarticle % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarNonrefereedarticle += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarNonrefereedarticles = farNonrefereedarticleProcessing.getTargetFarNonrefereedarticles( sesTarget )

# iRemoveFarNonrefereedarticle = 1
# for tgtMissingFarNonrefereedarticle in tgtMissingFarNonrefereedarticles:
# 	try:
# 		removeFarNonrefereedarticle = farNonrefereedarticleProcessing.softDeleteFarNonrefereedarticle( tgtMissingFarNonrefereedarticle, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarNonrefereedarticle )
# 		if iRemoveFarNonrefereedarticle % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarNonrefereedarticle += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farNonrefereedarticleProcessing
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  farEditedbookProcessing

# import farEditedbookProcessing

# srcFarEditedbooks = farEditedbookProcessing.getSourceFarEditedbooks( sesSource )

# iFarEditedbook = 1
# for srcFarEditedbook in srcFarEditedbooks:
# 	try:
# 		processedfarEditedbook = farEditedbookProcessing.processFarEditedbook( srcFarEditedbook, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarEditedbook )
# 		if iFarEditedbook % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarEditedbook += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarEditedbooks = farEditedbookProcessing.getTargetFarEditedbooks( sesTarget )

# iRemoveFarEditedbook = 1
# for tgtMissingFarEditedbook in tgtMissingFarEditedbooks:
# 	try:
# 		removeFarEditedbook = farEditedbookProcessing.softDeleteFarEditedbook( tgtMissingFarEditedbook, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarEditedbook )
# 		if iRemoveFarEditedbook % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarEditedbook += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farEditedbookProcessing
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  farBookChapterProcessing

# import farBookChapterProcessing

# srcFarBookChapters = farBookChapterProcessing.getSourceFarBookChapters( sesSource )

# iFarBookChapter = 1
# for srcFarBookChapter in srcFarBookChapters:
# 	try:
# 		processedfarBookChapter = farBookChapterProcessing.processFarBookChapter( srcFarBookChapter, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( processedfarBookChapter )
# 		if iFarBookChapter % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarBookChapter += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarBookChapters = farBookChapterProcessing.getTargetFarBookChapters( sesTarget )

# iRemoveFarBookChapter = 1
# for tgtMissingFarBookChapter in tgtMissingFarBookChapters:
# 	try:
# 		removeFarBookChapter = farBookChapterProcessing.softDeleteFarBookChapter( tgtMissingFarBookChapter, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarBookChapter )
# 		if iRemoveFarBookChapter % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarBookChapter += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farBookChapterProcessing
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  farBookReviewProcessing

# import farBookReviewProcessing

# srcFarBookReviews = farBookReviewProcessing.getSourceFarBookReviews( sesSource )

# iFarBookReview = 1
# for srcFarBookReview in srcFarBookReviews:
# 	try:
# 		frocessedFarBookReview = farBookReviewProcessing.processFarBookReview( srcFarBookReview, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( frocessedFarBookReview )
# 		if iFarBookReview % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarBookReview += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarBookReviews = farBookReviewProcessing.getTargetFarBookReviews( sesTarget )

# iRemoveFarBookReview = 1
# for tgtMissingFarBookReview in tgtMissingFarBookReviews:
# 	try:
# 		removeFarBookReview = farBookReviewProcessing.softDeleteFarBookReview( tgtMissingFarBookReview, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarBookReview )
# 		if iRemoveFarBookReview % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarBookReview += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farBookReviewProcessing
# ###############################################################################

# ###############################################################################
# # 
# #   File Import:  farEncyclopediaarticleProcessing

# import farEncyclopediaarticleProcessing

# srcFarEncyclopediaarticles = farEncyclopediaarticleProcessing.getSourceFarEncyclopediaarticles( sesSource )

# iFarEncyclopediaarticle = 1
# for srcFarEncyclopediaarticle in srcFarEncyclopediaarticles:
# 	try:
# 		frocessedFarEncyclopediaarticle = farEncyclopediaarticleProcessing.processFarEncyclopediaarticle( srcFarEncyclopediaarticle, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( frocessedFarEncyclopediaarticle )
# 		if iFarEncyclopediaarticle % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarEncyclopediaarticle += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarEncyclopediaarticles = farEncyclopediaarticleProcessing.getTargetFarEncyclopediaarticles( sesTarget )

# iRemoveFarEncyclopediaarticle = 1
# for tgtMissingFarEncyclopediaarticle in tgtMissingFarEncyclopediaarticles:
# 	try:
# 		removeFarEncyclopediaarticle = farEncyclopediaarticleProcessing.softDeleteFarEncyclopediaarticle( tgtMissingFarEncyclopediaarticle, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarEncyclopediaarticle )
# 		if iRemoveFarEncyclopediaarticle % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarEncyclopediaarticle += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farEncyclopediaarticleProcessing
# ###############################################################################


# ###############################################################################
# # 
# #   File Import:  farShortstoriesProcessing

# import farShortstoriesProcessing

# srcFarShortstories = farShortstoriesProcessing.getSourceFarShortstories( sesSource )

# iFarShortstorie = 1
# for srcFarShortstorie in srcFarShortstories:
# 	try:
# 		frocessedFarShortstories = farShortstoriesProcessing.processFarShortstorie( srcFarShortstorie, sesTarget )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( frocessedFarShortstories )
# 		if iFarShortstorie % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iFarShortstorie += 1
# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e


# tgtMissingFarShortstories = farShortstoriesProcessing.getTargetFarShortstories( sesTarget )

# iRemoveFarShortstorie = 1
# for tgtMissingFarShortstorie in tgtMissingFarShortstories:
# 	try:
# 		removeFarShortstorie = farShortstoriesProcessing.softDeleteFarShortstorie( tgtMissingFarShortstorie, sesSource )
# 	except TypeError as e:
# 		pass
# 	else:
# 		sesTarget.add( removeFarShortstorie )
# 		if iRemoveFarShortstorie % 1000 == 0:
# 			try:
# 				sesTarget.flush()
# 			except Exception as e:
# 				sesTarget.rollback()
# 				raise e
# 		iRemoveFarShortstorie += 1

# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e

# #	End of farShortstoriesProcessing
# ###############################################################################


# try:
# 	sesTarget.commit()
# except Exception as e:
# 	sesTarget.rollback()
# 	raise e
# finally:
# 	sesTarget.close()
# 	sesSource.close()

bioetlAppRun.cleanUp()


# End the processing of person addresses records
###############################################################################
