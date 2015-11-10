import datetime

import sharedProcesses

from app.connectdb import EtlConnections

bioetlAppRun = EtlConnections("bioetl")

sesSource = bioetlAppRun.getSourceSession()
sesTarget = bioetlAppRun.getTargetSession()

###############################################################################
# Load the mysql.bio_ps people table in into the final destination:
#	mysql:
#		bio_ps.people to bio_public.people
# 
import personProcessing

srcPeople = personProcessing.getSourcePeople( sesSource )

iPerson = 1
for srcPerson in srcPeople:
	try:
		personStatus = personProcessing.processPerson( srcPerson, sesTarget )
	except TypeError as e:
		pass
	else:
		sesTarget.add( personStatus )
		if iPerson % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
			except RuntimeError as e:
				pass
		iPerson += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# 
# "REMOVE" with a soft delete of person records no longer found in the source database
tgtMissingPeople = personProcessing.getTargetPeople( sesTarget )

iMissingPerson = 1
for tgtMissingPerson in tgtMissingPeople:
	try:
		personMissing = personProcessing.softDeletePerson( tgtMissingPerson, sesSource )
	except TypeError as e:
		# print e
		pass
	else:
		sesTarget.add( personMissing )
		if iMissingPerson % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iMissingPerson += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e
# End of the process for the person data 
###############################################################################


###############################################################################
# Load the mysql.bio_ps people data table in into the final destination:
#	mysql:
#		bio_ps.person_phones to bio_public.person_phones
# 	
# Using Group By on the source to limit likely duplicates.
#
import personPhoneProcessing

srcPersonPhones = personPhoneProcessing.getSourcePhones( sesSource )

iPersonPhone = 1
for srcPersonPhone in srcPersonPhones:
	try:
		processPhone = personPhoneProcessing.processPhone( srcPersonPhone, sesTarget )
	except Exception as e:
		# print e
		# raise e
		pass
	else:
		sesTarget.add( processPhone )
		if iPersonPhone % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonPhone += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# Remove the data that was no longer was found for an active person.
tgtMissingPersonPhones = personPhoneProcessing.getTargetPhones( sesTarget )

iRemovePhone = 1
for tgtMissingPersonPhone in tgtMissingPersonPhones:
	# if the phone no longer is found we remove it but only if the person is active...
	try:
		removePhone = personPhoneProcessing.cleanupSourcePhones( tgtMissingPersonPhone, sesSource )
	except TypeError, e:
		# print e
		pass
	else:
		sesTarget.add( tgtMissingPersonPhone )
		if iRemovePhone % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemovePhone += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# reset the updated_flag for all records for the next round of changes.
# engineTarget.execute("UPDATE person_phones SET updated_flag = 0;")
try:
	sharedProcesses.resetUpdatedFlag( sesTarget, "person_phones" )
except Exception as e:
	print e

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e


###############################################################################
# Load the mysql.bio_ps people data table in into the final destination:
#	mysql:
#		bio_ps.person_addresses to bio_public.person_addresses
# 	
# Using Group By on the source to limit likely duplicates.
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
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonAddresses += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# REMOVE the data no longer found in the source database...
tgtMissingPersonAddresses = personAddressProcessing.getTargetAddresses( sesTarget )

iRemoveAddresses = 1
for tgtMissingPersonAddress in tgtMissingPersonAddresses:
	try:
		removePersonAddress = personAddressProcessing.cleanupSourceAddresses( tgtMissingPersonAddress, sesSource )
	except TypeError as e:
		pass
	else:
		sesTarget.add( tgtMissingPersonAddress )
		if iRemoveAddresses % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemoveAddresses += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# RESET the updated_flag for all records for the next round of changes.
try:
	sharedProcesses.resetUpdatedFlag( sesTarget, "person_addresses" )
except Exception as e:
	print e

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

###############################################################################
# 
#   personWebProfileProcessing


import personWebProfileProcessing

srcPersonWebProfiles = personWebProfileProcessing.getSourcePersonWebProfile( sesSource )

iPersonWebProfile = 1
for srcPersonWebProfile in srcPersonWebProfiles:
	try:
		processedpersonWebProfile = personWebProfileProcessing.processPersonWebProfile( srcPersonWebProfile, sesTarget )
	except TypeError as e:
		pass
	else:
		sesTarget.add( processedpersonWebProfile )
		if iPersonWebProfile % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonWebProfile += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e


tgtMissingPersonWebProfiles = personWebProfileProcessing.getTargetPersonWebProfiles( sesTarget )

iRemove_Y_ = 1
for tgtMissingPersonWebProfile in tgtMissingPersonWebProfiles:
	try:
		removePersonWebProfile = personWebProfileProcessing.softDeletePersonWebProfile( tgtMissingPersonWebProfile, sesSource )
	except TypeError as e:
		pass
	else:
		sesTarget.add( removePersonWebProfile )
		if iRemove_Y_ % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemove_Y_ += 1

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# 	End of personWebProfileProcessing
###############################################################################


###############################################################################
# 
#   File Import:  personExternalLinkProcessing

import personExternalLinkProcessing

srcPersonExternalLinks = personExternalLinkProcessing.getSourcePersonExternalLinks( sesSource )

iPersonExternalLink = 1
for srcPersonExternalLink in srcPersonExternalLinks:
	try:
		processedpersonExternalLink = personExternalLinkProcessing.processPersonExternalLink( srcPersonExternalLink, sesTarget )
	except TypeError as e:
		pass
	else:
		sesTarget.add( processedpersonExternalLink )
		if iPersonExternalLink % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonExternalLink += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e


tgtMissingPersonExternalLinks = personExternalLinkProcessing.getTargetPersonExternalLinks( sesTarget )

iRemovePersonExternalLink = 1
for tgtMissingPersonExternalLink in tgtMissingPersonExternalLinks:
	try:
		removePersonExternalLink = personExternalLinkProcessing.softDeletePersonExternalLink( tgtMissingPersonExternalLink, sesSource )
	except TypeError as e:
		pass
	else:
		sesTarget.add( removePersonExternalLink )
		if iRemovePersonExternalLink % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemovePersonExternalLink += 1

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

#	End of personExternalLinkProcessing
###############################################################################

###############################################################################
# 
# xxxx

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
			except Exception as e:
				sesTarget.rollback()
				raise e
		iDepartment += 1

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e


tgtMissingDepartments = departmentProcessing.getTargetDepartments( sesTarget )

iRemoveDepartment = 1
for tgtMissingDepartment in tgtMissingDepartments:
	try:
		removeDepartment = departmentProcessing.softDeleteDepartment( tgtMissingDepartment )
	except TypeError as e:
		pass
	else:
		sesTarget.add( removeDepartment )
		if iRemoveDepartment % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemoveDepartment += 1

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# end of for tgtMissingDepartments
###############################################################################

###############################################################################
# 
#
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
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonJob += 1

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

tgtMissingPersonJobs = personJobsProcessing.getTargetPersonJobs( sesTarget )

iRemovePersonJob = 1
for tgtMissingPersonJob in tgtMissingPersonJobs:
	try:
		removePersonJob = personJobsProcessing.softDeletePersonJob( tgtMissingPersonJob, sesSource )
	except TypeError as e:
		pass
	else:
		sesTarget.add( removePersonJob )
		if iRemovePersonJob % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemovePersonJob += 1

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# end of for tgtMissingPersonJobs
###############################################################################

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e
finally:
	sesTarget.close()
	sesSource.close()

	bioetlAppRun.cleanUp()


# End the processing of person addresses records
###############################################################################
