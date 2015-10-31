from models.asudwpsmodels import AsuDwPsPerson, AsuPsBioFilters
from models.biopsmodels import BioPsPeople, BioPsPersonExternalLinks, BioPsPersonWebProfile

from sharedProcesses import hashThisList

# the data pull
def getSourcePerson( sesSource ):
	"""
		Selects the data from the data wharehouse for the AsuDwPsPerson model.
		@return: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

	return sesSource.query( 
		AsuDwPsPerson ).join(
			srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by(
				AsuDwPsPerson.emplid).all()
	

# the data load
def processPersonData( srcPerson ):
	"""
		Process an AsuDwPsPerson object and prepare it for insert into the target BioPsPeople table
		@return: the sa add object 
	"""
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
		srcPerson.last_update ]

	personHash = hashThisList( personList )

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
		last_update = srcPerson.last_update )

	return tgtPerson

def processPersonExternalLinksData( srcPerson ):
	"""
		A sub table of the AsuDwPsPerson table within the BioPs database.  The data is spares,
		and there for should be filtered.
		@return: sa object for add method OR None.
	"""
	personLinksList = [
		srcPerson.emplid,
		srcPerson.facebook,
		srcPerson.twitter,
		srcPerson.google_plus,
		srcPerson.linkedin ]

	if any( personLinksList[1:] ):
		personLinksHash = hashThisList( personLinksList )
	
		tgtPersonLinks = BioPsPersonExternalLinks(
			source_hash = personLinksHash,
			emplid = srcPerson.emplid,
			facebook = srcPerson.facebook,
			twitter = srcPerson.twitter,
			google_plus = srcPerson.google_plus,
			linkedin = srcPerson.linkedin )
	
		return tgtPersonLinks
	else:
		return None

def processPersonWebProfile( srcPerson ):
	"""
		Another sub table of the AsuDwPsPerson table with the BioPs database.  The data is spares,
		and there for should be filtered.
		@return: sa object for add method OR None
	"""
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
		personWebProfileHash = hashThisList( personWebProfileList )

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
		
		return personWebProfile
	else:
		return None

