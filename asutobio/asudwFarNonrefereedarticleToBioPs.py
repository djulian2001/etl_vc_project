from models.asudwpsmodels import AsuDwPsFarNonrefereedarticles, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarNonrefereedarticles

from sharedProcesses import hashThisList

# FarNonrefereedarticles  : first char caps plural,
# FarNonrefereedarticle caps singular,  
# farNonrefereedarticle lower case singular
# FAR_NONREFEREEDARTICLES all caps w/ underscore, -oracle table name
# ASUDW all caps w/ underscore, -oracle schema name
# far_nonrefereedarticles lower case w/ undersores - mysql table name

# bioetl\ filename	farNonrefereedarticleProcessing.py

# the data pull
def getSourceFarNonrefereedarticlesData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarNonrefereedarticles model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarNonrefereedarticles ).join(
			farEvals, AsuDwPsFarNonrefereedarticles.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarNonrefereedarticles.ispublic != 'N' ).all()

# the data load
def processFarNonrefereedarticleData( farNonrefereedarticle ):
	"""
		Process an AsuDwPsFarNonrefereedarticles object and prepare it for insert into the target BioPsFarNonrefereedarticles table
		@return: the sa add object 
	"""
	
	farNonrefereedarticleList = [
		farNonrefereedarticle.nonrefereedarticleid,
		farNonrefereedarticle.src_sys_id,
		farNonrefereedarticle.evaluationid,
		farNonrefereedarticle.authors,
		farNonrefereedarticle.title,
		farNonrefereedarticle.journalname,
		farNonrefereedarticle.publicationstatuscode,
		farNonrefereedarticle.publicationyear,
		farNonrefereedarticle.volumenumber,
		farNonrefereedarticle.pages,
		farNonrefereedarticle.webaddress,
		farNonrefereedarticle.translated,
		farNonrefereedarticle.abstract,
		farNonrefereedarticle.additionalinfo,
		farNonrefereedarticle.dtcreated,
		farNonrefereedarticle.dtupdated,
		farNonrefereedarticle.userlastmodified,
		farNonrefereedarticle.ispublic,
		farNonrefereedarticle.activityid,
		farNonrefereedarticle.load_error,
		farNonrefereedarticle.data_origin,
		farNonrefereedarticle.created_ew_dttm,
		farNonrefereedarticle.lastupd_dw_dttm,
		farNonrefereedarticle.batch_sid ]

	farNonrefereedarticleHash = hashThisList( farNonrefereedarticleList )

	tgtFarNonrefereedarticle = BioPsFarNonrefereedarticles(
		source_hash = farNonrefereedarticleHash,
		nonrefereedarticleid = farNonrefereedarticle.nonrefereedarticleid,
		src_sys_id = farNonrefereedarticle.src_sys_id,
		evaluationid = farNonrefereedarticle.evaluationid,
		authors = farNonrefereedarticle.authors,
		title = farNonrefereedarticle.title,
		journalname = farNonrefereedarticle.journalname,
		publicationstatuscode = farNonrefereedarticle.publicationstatuscode,
		publicationyear = farNonrefereedarticle.publicationyear,
		volumenumber = farNonrefereedarticle.volumenumber,
		pages = farNonrefereedarticle.pages,
		webaddress = farNonrefereedarticle.webaddress,
		translated = farNonrefereedarticle.translated,
		abstract = farNonrefereedarticle.abstract,
		additionalinfo = farNonrefereedarticle.additionalinfo,
		dtcreated = farNonrefereedarticle.dtcreated,
		dtupdated = farNonrefereedarticle.dtupdated,
		userlastmodified = farNonrefereedarticle.userlastmodified,
		ispublic = farNonrefereedarticle.ispublic,
		activityid = farNonrefereedarticle.activityid,
		load_error = farNonrefereedarticle.load_error,
		data_origin = farNonrefereedarticle.data_origin,
		created_ew_dttm = farNonrefereedarticle.created_ew_dttm,
		lastupd_dw_dttm = farNonrefereedarticle.lastupd_dw_dttm,
		batch_sid = farNonrefereedarticle.batch_sid )

	return tgtFarNonrefereedarticle
