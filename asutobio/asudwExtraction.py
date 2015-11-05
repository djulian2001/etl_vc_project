from app.connectdb import EtlConnections

# from sqlalchemy import *
# import cx_Oracle

# from models.asudwpsmodels import *
# from models.biopsmodels import *


###############################################################################
# application connection manager:
# 	imports connections to source and target, pass in the application scope
#	and the manager scopes how the application will be used.
asuDwPsAppRun = EtlConnections("asutobio")


try:

	sesSource = asuDwPsAppRun.getSourceSession()
	sesTarget = asuDwPsAppRun.getTargetSession()



###############################################################################
# Extract the oracle person table and cut up into:
#	mysql:
#		people
#		person_externallinks
#		person_webprofile, 
# 	


	import asudwPeopleToBioPs

	srcPersons = asudwPeopleToBioPs.getSourcePerson( sesSource )

	iPerson = 1
	for srcPerson in srcPersons:

		addPerson = asudwPeopleToBioPs.processPersonData( srcPerson )
		sesTarget.add( addPerson )

		addPersonExtLinks = asudwPeopleToBioPs.processPersonExternalLinksData( srcPerson )
		if addPersonExtLinks:
			sesTarget.add( addPersonExtLinks )

		addPersonWebProfile = asudwPeopleToBioPs.processPersonWebProfile( srcPerson )
		if addPersonWebProfile:
			sesTarget.add( addPersonWebProfile )

		if iPerson % 333 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				# sqlalchemy.orm.exc.FlushError might be a better...
				sesTarget.rollback()
				raise e
		
		iPerson += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e

#
# end of for srcPersons
###############################################################################

###############################################################################
# Extract the oracle person addresses table and cut up into:
# 	oracle:
#		filter on 'CLOC'  campus location (only)
#	mysql:
#		person_addresses
#

	import asudwAddressToBioPs
	
	srcPersonAddresses = asudwAddressToBioPs.getSourceAddresses( sesSource )

	iPersonAddress = 1
	for personAddress in srcPersonAddresses:
		
		addPersonAddress = asudwAddressToBioPs.processAddressesData( personAddress )
		
		sesTarget.add( addPersonAddress )

		if iPersonAddress % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e

		iPersonAddress += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e

#
# end of for srcPersonAddresses
###############################################################################


###############################################################################
# Extract the oracle person phones table and cut up into:
# 	oracle:
#		filter on list 'CELL','WORK' 
#	mysql:
#		bio_ps.person_phones
#

	import asudwPhonesToBioPs

	srcPersonPhones = asudwPhonesToBioPs.getSourcePhonesData( sesSource )

	iPersonPhones = 1
	for personPhone in srcPersonPhones:
		
		addPersonPhone = asudwPhonesToBioPs.processPhonesData( personPhone )
		sesTarget.add( addPersonPhone )

		if iPersonPhones % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e

		iPersonPhones += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e

#
# end of for srcPersonPhones
###############################################################################

###############################################################################
# Extract the oracle person jobs table and cut up into:
# 	oracle:
#		
#	mysql:
#		bio_ps.person_jobs
#

	import asudwJobsToBioPs
	srcPersonJobs = asudwJobsToBioPs.getSourceJobsData( sesSource )

	iPersonJobs = 1

	for personJob in srcPersonJobs:
		
		addPersonJob = asudwJobsToBioPs.processJobsData( personJob )
		sesTarget.add( addPersonJob )

		if iPersonJobs % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e

		iPersonJobs += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e

#
# end of for srcPersonJobs
###############################################################################



###############################################################################
# Extract the oracle person jobs table and cut up into:
# 	oracle:
#		
#	mysql:
#		bio_ps.person_subaffiliations
#

	import asudwSubAffiliationsToBioPs
	srcPersonSubAffiliations = asudwSubAffiliationsToBioPs.getSourceSubAffiliationsData( sesSource )

	iPersonSubAffiliations = 1
	for personSubAffiliation in srcPersonSubAffiliations:
		
		addPersonSubAffiliation = asudwSubAffiliationsToBioPs.processSubAffiliationsData( personSubAffiliation )
		sesTarget.add( addPersonSubAffiliation )

		if iPersonSubAffiliations % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e

		iPersonSubAffiliations += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e

#
# end of for srcPersonSubAffiliations
###############################################################################

###############################################################################
# Extract the oracle person jobs table and cut up into:
# 	oracle:
#		
#	mysql:
#		bio_ps.departments
#
	import asudwDepartmentsToBioPs

	srcDepartments = asudwDepartmentsToBioPs.getSourceDepartmentsData( sesSource )

	iDepartments = 1
	for srcDepartment in srcDepartments:

		addDepartment = asudwDepartmentsToBioPs.processDepartmentsData( srcDepartment )
		sesTarget.add( addDepartment )

		if iDepartments % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e

		iDepartments += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e
#
# end of for srcDepartments
###############################################################################



	# try:
	# 	sesTarget.commit()
	# except Exception as e:
	# 	sesTarget.rollback()
	# 	raise e
	# finally:
	# 	sesTarget.close()
	# 	sesSource.close()

	# 	asuDwPsAppRun.cleanUp()

except Exception as e:
	raise e
finally:
	sesTarget.close()
	sesSource.close()

	asuDwPsAppRun.cleanUp()