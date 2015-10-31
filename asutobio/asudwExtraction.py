from sqlalchemy import *
# import cx_Oracle

from models.asudwpsmodels import *
from models.biopsmodels import *

from app.connectdb import EtlConnections
from sharedProcesses import hashThisList

asuDwPsAppRun = EtlConnections("asutobio")

sesSource = asuDwPsAppRun.getSourceSession()
sesTarget = asuDwPsAppRun.getTargetSession()

true, false = literal(True), literal(False)

# sesTarget = TgtSessions()
# sesSource = SrcSessions()


####### TIME TO REFACTOR ########

# the source database sub query object used to extract all emplid's for biodesign data... 
srcFilters = AsuPsBioFilters( sesSource ) # remove after the refactoring has been completed

srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

# print srcFilters.getAllBiodesignEmplidList(False)

# ###############################################################################
# # Utilitie Functions:
# ###############################################################################
# def hashThisList(theList):
# 	"""
# 		The following takes in a list of variable data types, casts them to a
# 		string, then concatenates them together, then hashs the string value
# 		and returns it.
# 	"""
# 	thisString = ""
# 	for i in theList:
# 		thisString += str(i)

# 	thisSha256Hash = hashlib.sha256(thisString).hexdigest()

# 	return thisSha256Hash



###############################################################################
# Extract the oracle person table and cut up into:
#	mysql:
#		people
#		person_externallinks
#		person_webprofile, 
# 	

# srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid)[1:5]
# srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid).all()
# sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid).all()
try:
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



except Exception as e:
	asuDwPsAppRun.cleanUp()
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

srcPersonPhones = sesSource.query(
	AsuDwPsPhones ).join(
		srcEmplidsSubQry, AsuDwPsPhones.emplid==srcEmplidsSubQry.c.emplid ).filter(
			AsuDwPsPhones.phone_type.in_( 
				['CELL','WORK'] ) ).order_by(
					AsuDwPsPhones.emplid ).all()

iPersonPhones = 1

for personPhone in srcPersonPhones:
	
	personPhoneList = [
		personPhone.emplid,
		personPhone.phone_type,
		personPhone.phone,
		personPhone.last_update]

	personPhoneHash = hashThisList(personPhoneList)

	tgtPersonPhone = BioPsPhones(
		source_hash = personPhoneHash,
		emplid = personPhone.emplid,
		phone_type = personPhone.phone_type,
		phone = personPhone.phone,
		last_update = personPhone.last_update )

	sesTarget.add( tgtPersonPhone )

	if iPersonPhones % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e

	iPersonPhones += 1

try:
	sesTarget.commit()
except Exception, e:
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

srcPersonJobs = sesSource.query(
	AsuDwPsJobs ).join(
		srcEmplidsSubQry, AsuDwPsJobs.emplid==srcEmplidsSubQry.c.emplid ).order_by(
			AsuDwPsJobs.emplid ).all()

iPersonJobs = 1

for personJob in srcPersonJobs:
	
	personJobList = [
		personJob.emplid,
		personJob.empl_rcd,
		personJob.title,
		personJob.department,
		personJob.mailcode,
		personJob.empl_class,
		personJob.job_indicator,
		personJob.location,
		personJob.hr_status,
		personJob.deptid,
		personJob.empl_status,
		personJob.fte,
		personJob.last_update,
		personJob.department_directory]

	personJobHash = hashThisList(personJobList)

	tgtPersonJob = BioPsJobs(
		source_hash = personJobHash,
		emplid = personJob.emplid,
		empl_rcd = personJob.empl_rcd,
		title = personJob.title,
		department = personJob.department,
		mailcode = personJob.mailcode,
		empl_class = personJob.empl_class,
		job_indicator = personJob.job_indicator,
		location = personJob.location,
		hr_status = personJob.hr_status,
		deptid = personJob.deptid,
		empl_status = personJob.empl_status,
		fte = personJob.fte,
		last_update = personJob.last_update,
		department_directory = personJob.department_directory)

	sesTarget.add( tgtPersonJob )

	if iPersonJobs % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e

	iPersonJobs += 1

try:
	sesTarget.commit()
except Exception, e:
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

srcPersonSubAffiliations = sesSource.query(
	AsuDwPsSubAffiliations ).join(
		srcEmplidsSubQry, AsuDwPsSubAffiliations.emplid==srcEmplidsSubQry.c.emplid ).order_by(
			AsuDwPsSubAffiliations.emplid ).all()

iPersonSubAffiliations = 1

for personSubAffiliation in srcPersonSubAffiliations:
	
	personSubAffiliationList = [
		personSubAffiliation.emplid,
		personSubAffiliation.deptid,
		personSubAffiliation.subaffiliation_code,
		personSubAffiliation.campus,
		personSubAffiliation.title,
		personSubAffiliation.short_description,
		personSubAffiliation.description,
		personSubAffiliation.directory_publish,
		personSubAffiliation.department,
		personSubAffiliation.last_update,
		personSubAffiliation.department_directory ]

	personSubAffiliationHash = hashThisList( personSubAffiliationList )

	tgtPersonSubAffiliation = BioPsSubAffiliations(
		source_hash = personSubAffiliationHash,
		emplid = personSubAffiliation.emplid,
		deptid = personSubAffiliation.deptid,
		subaffiliation_code = personSubAffiliation.subaffiliation_code,
		campus = personSubAffiliation.campus,
		title = personSubAffiliation.title,
		short_description = personSubAffiliation.short_description,
		description = personSubAffiliation.description,
		directory_publish = personSubAffiliation.directory_publish,
		department = personSubAffiliation.department,
		last_update = personSubAffiliation.last_update,
		department_directory = personSubAffiliation.department_directory )

	sesTarget.add( tgtPersonSubAffiliation )

	if iPersonSubAffiliations % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e

	iPersonSubAffiliations += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e

#
# end of for srcPersonJobs
###############################################################################


# print srcPersons

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e
finally:
	sesTarget.close()
	sesSource.close()

	asuDwPsAppRun.cleanUp()

# for this in emplid_list:
# 	print this.emplid

# id_list = sesSource.execute(sql).fetchall()
# for an_id in id_list:
# 	print an_id