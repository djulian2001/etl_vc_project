from sqlalchemy import *
import hashlib
import cx_Oracle
# from sqlalchemy.engine import reflection
from models.asudwpsmodels import *
from models.biopsmodels import *

# source engine
sourceDbUser = "ASU_BDI_EXTRACT_APP"
sourceDbPw = "np55adW_G1_Um-ii"
sourceDbNetServiceName = "ASUPMDEV"
engSourceString = 'oracle+cx_oracle://%s:%s@%s' % (sourceDbUser, sourceDbPw, sourceDbNetServiceName)
engineSource = create_engine( engSourceString, echo=True )

# target engine
targetDbUser = 'app_asutobioetl'
targetDbPw = 'forthegipperNotReagan4show'
targetDbHost = 'dbdev.biodesign.asu.edu'
targetDbName = 'bio_ps'
engTargetString = 'mysql+mysqldb://%s:%s@%s/%s' % (targetDbUser, targetDbPw, targetDbHost, targetDbName)
engineTarget = create_engine( engTargetString, echo=True )

# Drop and Recreate the database objects and data.....
BioPs.metadata.drop_all(engineTarget)

BioPs.metadata.create_all(engineTarget)

# Get the ORM layer in place to start using the models to query from our existing databases...
BioPs.metadata.bind = engineTarget
AsuDwPs.metadata.bind = engineSource

TgtSessions = scoped_session(sessionmaker(bind=engineTarget))
SrcSessions = scoped_session(sessionmaker(bind=engineSource))

true, false = literal(True), literal(False)

sesTarget = TgtSessions()
sesSource = SrcSessions()

# the source database sub query object used to extract all emplid's for biodesign data... 
srcFilters = AsuPsBioFilters(sesSource)

srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

print srcFilters.getAllBiodesignEmplidList(False)

###############################################################################
# Utilitie Functions:
###############################################################################
def hashThisList(theList):
	"""
		The following takes in a list of variable data types, casts them to a
		string, then concatenates them together, then hashs the string value
		and returns it.
	"""
	thisString = ""
	for i in theList:
		thisString += str(i)

	thisSha256Hash = hashlib.sha256(thisString).hexdigest()

	return thisSha256Hash



###############################################################################
# Extract the oracle person table and cut up into:
#	mysql:
#		people
#		person_externallinks
#		person_webprofile, 
# 	

# srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid)[1:5]
# srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid).all()
srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid).all()

iPerson = 1
for srcPerson in srcPersons:

	# map AsuDwpsPerson the columns where they will be extracted to into the bio_ps database
	# person -> bio_ps.people
	personList = [
		srcPerson.emplid,
		srcPerson.asurite_id,
		srcPerson.asu_id,
		srcPerson.ferpa,
		srcPerson.last_name,
		srcPerson.first_name,
		srcPerson.middle_name,
		srcPerson.display_name,
		srcPerson.preferred_first_name,
		srcPerson.affiliations,
		srcPerson.email_address,
		srcPerson.eid,
		srcPerson.birthdate,
		srcPerson.last_update]

	personHash = hashThisList(personList)

	tgtPerson = BioPsPeople(
		source_hash = personHash,
		emplid = srcPerson.emplid,
		asurite_id = srcPerson.asurite_id,
		asu_id = srcPerson.asu_id,
		ferpa = srcPerson.ferpa,
		last_name = srcPerson.last_name,
		first_name = srcPerson.first_name,
		middle_name = srcPerson.middle_name,
		display_name = srcPerson.display_name,
		preferred_first_name = srcPerson.preferred_first_name,
		affiliations = srcPerson.affiliations,
		email_address = srcPerson.email_address,
		eid = srcPerson.eid,
		birthdate = srcPerson.birthdate,
		last_update = srcPerson.last_update)

	sesTarget.add(tgtPerson)

	# not required but needed data... no nulls
	personLinksList = [
		srcPerson.emplid,
		srcPerson.facebook,
		srcPerson.twitter,
		srcPerson.google_plus,
		srcPerson.linkedin
	]

	if any( personLinksList[1:] ):
		personLinksHash = hashThisList(personLinksList)
	
		tgtPersonLinks = BioPsPersonExternalLinks(
			source_hash = personLinksHash,
			emplid = srcPerson.emplid,
			facebook = srcPerson.facebook,
			twitter = srcPerson.twitter,
			google_plus = srcPerson.google_plus,
			linkedin = srcPerson.linkedin)
	
		sesTarget.add(tgtPersonLinks)

	personWebProfileList = [
		srcPerson.emplid,
		srcPerson.bio,
		srcPerson.research_interests,
		srcPerson.cv,
		srcPerson.website,
		srcPerson.teaching_website,
		srcPerson.grad_faculties,
		srcPerson.professional_associations,
		srcPerson.work_history,
		srcPerson.education,
		srcPerson.research_group,
		srcPerson.research_website,
		srcPerson.honors_awards,
		srcPerson.editorships,
		srcPerson.presentations]

	if any( personWebProfileList[1:] ):
		personWebProfileHash = hashThisList(personWebProfileList)

		personWebProfile = BioPsPersonWebProfile(
			source_hash = personWebProfileHash,
			emplid = srcPerson.emplid,
			bio = srcPerson.bio,
			research_interests = srcPerson.research_interests,
			cv = srcPerson.cv,
			website = srcPerson.website,
			teaching_website = srcPerson.teaching_website,
			grad_faculties = srcPerson.grad_faculties,
			professional_associations = srcPerson.professional_associations,
			work_history = srcPerson.work_history,
			education = srcPerson.education,
			research_group = srcPerson.research_group,
			research_website = srcPerson.research_website,
			honors_awards = srcPerson.honors_awards,
			editorships = srcPerson.editorships,
			presentations = srcPerson.presentations)

		sesTarget.add(personWebProfile)

	if iPerson % 333 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e
	
	iPerson += 1



try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e
# finally:
# 	sesTarget.close()
# 	sesSource.close()
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

srcPersonAddresses = sesSource.query(
	AsuDwPsAddresses ).join(
		srcEmplidsSubQry, AsuDwPsAddresses.emplid==srcEmplidsSubQry.c.emplid ).filter(
			AsuDwPsAddresses.address_type=='CLOC' ).order_by(
				AsuDwPsAddresses.emplid ).all()

iPersonAddress = 1

for personAddress in srcPersonAddresses:
	
	personAddressList = [
		personAddress.emplid,
		personAddress.address_type,
		personAddress.address1,
		personAddress.address2,
		personAddress.address3,
		personAddress.address4,
		personAddress.city,
		personAddress.state,
		personAddress.postal,
		personAddress.country_code,
		personAddress.country_descr,
		personAddress.last_update
	]

	personAddressHash = hashThisList(personAddressList)

	tgtPersonAddress = BioPsAddresses(
		source_hash = personAddressHash,
		emplid = personAddress.emplid,
		address_type = personAddress.address_type,
		address1 = personAddress.address1,
		address2 = personAddress.address2,
		address3 = personAddress.address3,
		address4 = personAddress.address4,
		city = personAddress.city,
		state = personAddress.state,
		postal = personAddress.postal,
		country_code = personAddress.country_code,
		country_descr = personAddress.country_descr,
		last_update = personAddress.last_update)

	sesTarget.add(tgtPersonAddress)


	if iPersonAddress % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e

	iPersonAddress += 1

try:
	sesTarget.commit()
except Exception, e:
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



# print srcPersons

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e
finally:
	sesTarget.close()
	sesSource.close()



# for this in emplid_list:
# 	print this.emplid

# id_list = sesSource.execute(sql).fetchall()
# for an_id in id_list:
# 	print an_id