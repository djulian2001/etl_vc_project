import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarRefereedarticles, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarRefereedarticles, AsuDwPsFarEvaluations, AsuPsBioFilters

def getSourceFarRefereedarticles( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarRefereedarticles table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarRefereedarticles ).join(
			farEvals, AsuDwPsFarRefereedarticles.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarRefereedarticles.ispublic != 'N' ).all()

# change value to the singular
def processFarRefereedarticle( srcFarRefereedarticle, sesTarget ):
	"""
		Takes in a source FarRefereedarticle object from biopsmodels (mysql.bio_ps.FarRefereedarticles)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarRefereedarticles), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farRefereedarticleList = [
		srcFarRefereedarticle.refereedarticleid,
		srcFarRefereedarticle.src_sys_id,
		srcFarRefereedarticle.evaluationid,
		srcFarRefereedarticle.authors,
		srcFarRefereedarticle.title,
		srcFarRefereedarticle.journalname,
		srcFarRefereedarticle.publicationstatuscode,
		srcFarRefereedarticle.publicationyear,
		srcFarRefereedarticle.volumenumber,
		srcFarRefereedarticle.pages,
		srcFarRefereedarticle.webaddress,
		srcFarRefereedarticle.translated,
		srcFarRefereedarticle.abstract,
		srcFarRefereedarticle.additionalinfo,
		srcFarRefereedarticle.dtcreated,
		srcFarRefereedarticle.dtupdated,
		srcFarRefereedarticle.userlastmodified,
		srcFarRefereedarticle.ispublic,
		srcFarRefereedarticle.activityid,
		srcFarRefereedarticle.load_error,
		srcFarRefereedarticle.data_origin,
		srcFarRefereedarticle.created_ew_dttm,
		srcFarRefereedarticle.lastupd_dw_dttm,
		srcFarRefereedarticle.batch_sid ]

	srcHash = hashThisList( farRefereedarticleList )

	def farRefereedarticleExists():
		"""
			determine the farRefereedarticle exists in the target database.
			@True: The farRefereedarticle exists in the database
			@False: The farRefereedarticle does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarRefereedarticles.refereedarticleid == srcFarRefereedarticle.refereedarticleid ) )

		return ret

	if farRefereedarticleExists():

		def farRefereedarticleUpdateRequired():
			"""
				Determine if the farRefereedarticle that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarRefereedarticles.refereedarticleid == srcFarRefereedarticle.refereedarticleid ).where(
					FarRefereedarticles.source_hash == srcHash ).where(	
					FarRefereedarticles.deleted_at.is_( None ) ) )

			return not ret

		if farRefereedarticleUpdateRequired():
			# retrive the tables object to update.
			updateFarRefereedarticle = sesTarget.query(
				FarRefereedarticles ).filter(
					FarRefereedarticles.refereedarticleid == srcFarRefereedarticle.refereedarticleid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarRefereedarticle.source_hash = srcHash
			updateFarRefereedarticle.refereedarticleid = srcFarRefereedarticle.refereedarticleid
			updateFarRefereedarticle.src_sys_id = srcFarRefereedarticle.src_sys_id
			updateFarRefereedarticle.evaluationid = srcFarRefereedarticle.evaluationid
			updateFarRefereedarticle.authors = srcFarRefereedarticle.authors
			updateFarRefereedarticle.title = srcFarRefereedarticle.title
			updateFarRefereedarticle.journalname = srcFarRefereedarticle.journalname
			updateFarRefereedarticle.publicationstatuscode = srcFarRefereedarticle.publicationstatuscode
			updateFarRefereedarticle.publicationyear = srcFarRefereedarticle.publicationyear
			updateFarRefereedarticle.volumenumber = srcFarRefereedarticle.volumenumber
			updateFarRefereedarticle.pages = srcFarRefereedarticle.pages
			updateFarRefereedarticle.webaddress = srcFarRefereedarticle.webaddress
			updateFarRefereedarticle.translated = srcFarRefereedarticle.translated
			updateFarRefereedarticle.abstract = srcFarRefereedarticle.abstract
			updateFarRefereedarticle.additionalinfo = srcFarRefereedarticle.additionalinfo
			updateFarRefereedarticle.dtcreated = srcFarRefereedarticle.dtcreated
			updateFarRefereedarticle.dtupdated = srcFarRefereedarticle.dtupdated
			updateFarRefereedarticle.userlastmodified = srcFarRefereedarticle.userlastmodified
			updateFarRefereedarticle.ispublic = srcFarRefereedarticle.ispublic
			updateFarRefereedarticle.activityid = srcFarRefereedarticle.activityid
			updateFarRefereedarticle.load_error = srcFarRefereedarticle.load_error
			updateFarRefereedarticle.data_origin = srcFarRefereedarticle.data_origin
			updateFarRefereedarticle.created_ew_dttm = srcFarRefereedarticle.created_ew_dttm
			updateFarRefereedarticle.lastupd_dw_dttm = srcFarRefereedarticle.lastupd_dw_dttm
			updateFarRefereedarticle.batch_sid = srcFarRefereedarticle.batch_sid
			updateFarRefereedarticle.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarRefereedarticle.deleted_at = None

			return updateFarRefereedarticle

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarRefereedarticle.evaluationid ).one()

		insertFarRefereedarticle = FarRefereedarticles(
			source_hash = srcHash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			refereedarticleid = srcFarRefereedarticle.refereedarticleid,
			src_sys_id = srcFarRefereedarticle.src_sys_id,
			evaluationid = srcFarRefereedarticle.evaluationid,
			authors = srcFarRefereedarticle.authors,
			title = srcFarRefereedarticle.title,
			journalname = srcFarRefereedarticle.journalname,
			publicationstatuscode = srcFarRefereedarticle.publicationstatuscode,
			publicationyear = srcFarRefereedarticle.publicationyear,
			volumenumber = srcFarRefereedarticle.volumenumber,
			pages = srcFarRefereedarticle.pages,
			webaddress = srcFarRefereedarticle.webaddress,
			translated = srcFarRefereedarticle.translated,
			abstract = srcFarRefereedarticle.abstract,
			additionalinfo = srcFarRefereedarticle.additionalinfo,
			dtcreated = srcFarRefereedarticle.dtcreated,
			dtupdated = srcFarRefereedarticle.dtupdated,
			userlastmodified = srcFarRefereedarticle.userlastmodified,
			ispublic = srcFarRefereedarticle.ispublic,
			activityid = srcFarRefereedarticle.activityid,
			load_error = srcFarRefereedarticle.load_error,
			data_origin = srcFarRefereedarticle.data_origin,
			created_ew_dttm = srcFarRefereedarticle.created_ew_dttm,
			lastupd_dw_dttm = srcFarRefereedarticle.lastupd_dw_dttm,
			batch_sid = srcFarRefereedarticle.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarRefereedarticle

def getTargetFarRefereedarticles( sesTarget ):
	"""
		Returns a set of FarRefereedarticles objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		FarRefereedarticles ).filter(
			FarRefereedarticles.deleted_at.is_( None ) ).all()

def softDeleteFarRefereedarticle( tgtRecord, srcRecords ):
	"""
		The list of source records changes as time moves on, the source records
		removed from the list are not deleted, but flaged removed by the 
		deleted_at field.

		The return of this function returns a sqlalchemy object to update a target record object.
	"""
	def dataMissing():
		"""
			The origional list of selected data is then used to see if data requires a soft-delete
			@True: Means update the records deleted_at column
			@False: Do nothing
		"""
		return not any( srcRecord.refereedarticleid == tgtRecord.refereedarticleid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
