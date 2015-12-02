from models.asudwpsmodels import AsuDwPsFarEncyclopediaarticles, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarEncyclopediaarticles

from sharedProcesses import hashThisList

# the data pull
def getSourceFarEncyclopediaarticlesData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarEncyclopediaarticles model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarEncyclopediaarticles ).join(
			farEvals, AsuDwPsFarEncyclopediaarticles.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarEncyclopediaarticles.ispublic !='N' ).all()
	
# the data load
def processFarEncyclopediaarticleData( farEncyclopediaarticle ):
	"""
		Process an AsuDwPsFarEncyclopediaarticles object and prepare it for insert into the target BioPsFarEncyclopediaarticles table
		@return: the sa add object 
	"""
	
	farEncyclopediaarticleList = [
		farEncyclopediaarticle.encyclopediaarticleid,
		farEncyclopediaarticle.src_sys_id,
		farEncyclopediaarticle.evaluationid,
		farEncyclopediaarticle.authors,
		farEncyclopediaarticle.title,
		farEncyclopediaarticle.publicationname,
		farEncyclopediaarticle.publicationstatuscode,
		farEncyclopediaarticle.pages,
		farEncyclopediaarticle.publicationyear,
		farEncyclopediaarticle.publisher,
		farEncyclopediaarticle.webaddress,
		farEncyclopediaarticle.additionalinfo,
		farEncyclopediaarticle.dtcreated,
		farEncyclopediaarticle.dtupdated,
		farEncyclopediaarticle.userlastmodified,
		farEncyclopediaarticle.ispublic,
		farEncyclopediaarticle.activityid,
		farEncyclopediaarticle.load_error,
		farEncyclopediaarticle.data_origin,
		farEncyclopediaarticle.created_ew_dttm,
		farEncyclopediaarticle.lastupd_dw_dttm,
		farEncyclopediaarticle.batch_sid ]

	farEncyclopediaarticleHash = hashThisList( farEncyclopediaarticleList )

	tgtFarEncyclopediaarticle = BioPsFarEncyclopediaarticles(
		source_hash = farEncyclopediaarticleHash,
		encyclopediaarticleid = farEncyclopediaarticle.encyclopediaarticleid,
		src_sys_id = farEncyclopediaarticle.src_sys_id,
		evaluationid = farEncyclopediaarticle.evaluationid,
		authors = farEncyclopediaarticle.authors,
		title = farEncyclopediaarticle.title,
		publicationname = farEncyclopediaarticle.publicationname,
		publicationstatuscode = farEncyclopediaarticle.publicationstatuscode,
		pages = farEncyclopediaarticle.pages,
		publicationyear = farEncyclopediaarticle.publicationyear,
		publisher = farEncyclopediaarticle.publisher,
		webaddress = farEncyclopediaarticle.webaddress,
		additionalinfo = farEncyclopediaarticle.additionalinfo,
		dtcreated = farEncyclopediaarticle.dtcreated,
		dtupdated = farEncyclopediaarticle.dtupdated,
		userlastmodified = farEncyclopediaarticle.userlastmodified,
		ispublic = farEncyclopediaarticle.ispublic,
		activityid = farEncyclopediaarticle.activityid,
		load_error = farEncyclopediaarticle.load_error,
		data_origin = farEncyclopediaarticle.data_origin,
		created_ew_dttm = farEncyclopediaarticle.created_ew_dttm,
		lastupd_dw_dttm = farEncyclopediaarticle.lastupd_dw_dttm,
		batch_sid = farEncyclopediaarticle.batch_sid )

	return tgtFarEncyclopediaarticle
