import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarNonrefereedarticles, FarEvaluations
from asutobio.models.biopsmodels import BioPsFarNonrefereedarticles
from models.asudwpsmodels import AsuDwPsFarNonrefereedarticles, AsuDwPsFarEvaluations, AsuPsBioFilters

def getSourceFarNonrefereedarticles( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarNonrefereedarticles table of the source database.
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

# change value to the singular
def processFarNonrefereedarticle( srcFarNonrefereedarticle, sesTarget ):
	"""
		Takes in a source FarNonrefereedarticle object from biopsmodels (mysql.bio_ps.FarNonrefereedarticles)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarNonrefereedarticles), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farNonrefereedarticleList = [
		srcFarNonrefereedarticle.nonrefereedarticleid,
		srcFarNonrefereedarticle.src_sys_id,
		srcFarNonrefereedarticle.evaluationid,
		srcFarNonrefereedarticle.authors,
		srcFarNonrefereedarticle.title,
		srcFarNonrefereedarticle.journalname,
		srcFarNonrefereedarticle.publicationstatuscode,
		srcFarNonrefereedarticle.publicationyear,
		srcFarNonrefereedarticle.volumenumber,
		srcFarNonrefereedarticle.pages,
		srcFarNonrefereedarticle.webaddress,
		srcFarNonrefereedarticle.translated,
		srcFarNonrefereedarticle.abstract,
		srcFarNonrefereedarticle.additionalinfo,
		srcFarNonrefereedarticle.dtcreated,
		srcFarNonrefereedarticle.dtupdated,
		srcFarNonrefereedarticle.userlastmodified,
		srcFarNonrefereedarticle.ispublic,
		srcFarNonrefereedarticle.activityid,
		srcFarNonrefereedarticle.load_error,
		srcFarNonrefereedarticle.data_origin,
		srcFarNonrefereedarticle.created_ew_dttm,
		srcFarNonrefereedarticle.lastupd_dw_dttm,
		srcFarNonrefereedarticle.batch_sid ]

	srcHash = hashThisList( farNonrefereedarticleList )

	def farNonrefereedarticleExists():
		"""
			determine the farNonrefereedarticle exists in the target database.
			@True: The farNonrefereedarticle exists in the database
			@False: The farNonrefereedarticle does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarNonrefereedarticles.nonrefereedarticleid == srcFarNonrefereedarticle.nonrefereedarticleid ) )

		return ret

	if farNonrefereedarticleExists():
		def farNonrefereedarticleUpdateRequired():
			"""
				Determine if the farNonrefereedarticle that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarNonrefereedarticles.nonrefereedarticleid == srcFarNonrefereedarticle.nonrefereedarticleid ).where(
					FarNonrefereedarticles.source_hash == srcHash ).where(	
					FarNonrefereedarticles.deleted_at.is_( None ) ) )

			return not ret

		if farNonrefereedarticleUpdateRequired():
			# retrive the tables object to update.
			updateFarNonrefereedarticle = sesTarget.query(
				FarNonrefereedarticles ).filter(
					FarNonrefereedarticles.nonrefereedarticleid == srcFarNonrefereedarticle.nonrefereedarticleid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarNonrefereedarticle.source_hash = srcHash
			updateFarNonrefereedarticle.nonrefereedarticleid = srcFarNonrefereedarticle.nonrefereedarticleid
			updateFarNonrefereedarticle.src_sys_id = srcFarNonrefereedarticle.src_sys_id
			updateFarNonrefereedarticle.evaluationid = srcFarNonrefereedarticle.evaluationid
			updateFarNonrefereedarticle.authors = srcFarNonrefereedarticle.authors
			updateFarNonrefereedarticle.title = srcFarNonrefereedarticle.title
			updateFarNonrefereedarticle.journalname = srcFarNonrefereedarticle.journalname
			updateFarNonrefereedarticle.publicationstatuscode = srcFarNonrefereedarticle.publicationstatuscode
			updateFarNonrefereedarticle.publicationyear = srcFarNonrefereedarticle.publicationyear
			updateFarNonrefereedarticle.volumenumber = srcFarNonrefereedarticle.volumenumber
			updateFarNonrefereedarticle.pages = srcFarNonrefereedarticle.pages
			updateFarNonrefereedarticle.webaddress = srcFarNonrefereedarticle.webaddress
			updateFarNonrefereedarticle.translated = srcFarNonrefereedarticle.translated
			updateFarNonrefereedarticle.abstract = srcFarNonrefereedarticle.abstract
			updateFarNonrefereedarticle.additionalinfo = srcFarNonrefereedarticle.additionalinfo
			updateFarNonrefereedarticle.dtcreated = srcFarNonrefereedarticle.dtcreated
			updateFarNonrefereedarticle.dtupdated = srcFarNonrefereedarticle.dtupdated
			updateFarNonrefereedarticle.userlastmodified = srcFarNonrefereedarticle.userlastmodified
			updateFarNonrefereedarticle.ispublic = srcFarNonrefereedarticle.ispublic
			updateFarNonrefereedarticle.activityid = srcFarNonrefereedarticle.activityid
			updateFarNonrefereedarticle.load_error = srcFarNonrefereedarticle.load_error
			updateFarNonrefereedarticle.data_origin = srcFarNonrefereedarticle.data_origin
			updateFarNonrefereedarticle.created_ew_dttm = srcFarNonrefereedarticle.created_ew_dttm
			updateFarNonrefereedarticle.lastupd_dw_dttm = srcFarNonrefereedarticle.lastupd_dw_dttm
			updateFarNonrefereedarticle.batch_sid = srcFarNonrefereedarticle.batch_sid
			updateFarNonrefereedarticle.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarNonrefereedarticle.deleted_at = None

			return updateFarNonrefereedarticle

	else:
		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarNonrefereedarticle.evaluationid ).one()

		insertFarNonrefereedarticle = FarNonrefereedarticles(
			source_hash = srcHash,
			nonrefereedarticleid = srcFarNonrefereedarticle.nonrefereedarticleid,
			far_evaluation_id = srcGetFarEvaluationId.id,
			src_sys_id = srcFarNonrefereedarticle.src_sys_id,
			evaluationid = srcFarNonrefereedarticle.evaluationid,
			authors = srcFarNonrefereedarticle.authors,
			title = srcFarNonrefereedarticle.title,
			journalname = srcFarNonrefereedarticle.journalname,
			publicationstatuscode = srcFarNonrefereedarticle.publicationstatuscode,
			publicationyear = srcFarNonrefereedarticle.publicationyear,
			volumenumber = srcFarNonrefereedarticle.volumenumber,
			pages = srcFarNonrefereedarticle.pages,
			webaddress = srcFarNonrefereedarticle.webaddress,
			translated = srcFarNonrefereedarticle.translated,
			abstract = srcFarNonrefereedarticle.abstract,
			additionalinfo = srcFarNonrefereedarticle.additionalinfo,
			dtcreated = srcFarNonrefereedarticle.dtcreated,
			dtupdated = srcFarNonrefereedarticle.dtupdated,
			userlastmodified = srcFarNonrefereedarticle.userlastmodified,
			ispublic = srcFarNonrefereedarticle.ispublic,
			activityid = srcFarNonrefereedarticle.activityid,
			load_error = srcFarNonrefereedarticle.load_error,
			data_origin = srcFarNonrefereedarticle.data_origin,
			created_ew_dttm = srcFarNonrefereedarticle.created_ew_dttm,
			lastupd_dw_dttm = srcFarNonrefereedarticle.lastupd_dw_dttm,
			batch_sid = srcFarNonrefereedarticle.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarNonrefereedarticle

def getTargetFarNonrefereedarticles( sesTarget ):
	"""
		Returns a set of FarNonrefereedarticles objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		FarNonrefereedarticles ).filter(
			FarNonrefereedarticles.deleted_at.is_( None ) ).all()

def softDeleteFarNonrefereedarticle( tgtRecord, srcRecords ):
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
		return not any( srcRecord.nonrefereedarticleid == tgtRecord.nonrefereedarticleid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
