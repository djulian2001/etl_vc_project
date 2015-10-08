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
###############################################################################

# srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid)[1:5]
# srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid).all()
srcPersons = sesSource.query( AsuDwPsPerson ).join(srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by( AsuDwPsPerson.emplid)



print srcPersons

exit()

i = 1
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

	personLinksList = [
		srcPerson.emplid,
		srcPerson.facebook,
		srcPerson.twitter,
		srcPerson.google_plus,
		srcPerson.linkedin
	]

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


	if i % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e
	i += 1

# end of for srcPersons

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e
finally:
	sesTarget.close() 


# print srcPersons




# for this in emplid_list:
# 	print this.emplid

# id_list = sesSource.execute(sql).fetchall()
# for an_id in id_list:
# 	print an_id