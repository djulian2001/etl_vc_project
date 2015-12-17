import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarEncyclopediaarticles, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarEncyclopediaarticles, AsuDwPsFarEvaluations, AsuPsBioFilters

def getSourceFarEncyclopediaarticles( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarEncyclopediaarticles table of the source database.
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
	
# change value to the singular
def processFarEncyclopediaarticle( srcFarEncyclopediaarticle, sesTarget ):
	"""
		Takes in a source FarEncyclopediaarticle object from biopsmodels (mysql.bio_ps.FarEncyclopediaarticles)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarEncyclopediaarticles), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farEncyclopediaarticleList = [
		srcFarEncyclopediaarticle.encyclopediaarticleid,
		srcFarEncyclopediaarticle.src_sys_id,
		srcFarEncyclopediaarticle.evaluationid,
		srcFarEncyclopediaarticle.authors,
		srcFarEncyclopediaarticle.title,
		srcFarEncyclopediaarticle.publicationname,
		srcFarEncyclopediaarticle.publicationstatuscode,
		srcFarEncyclopediaarticle.pages,
		srcFarEncyclopediaarticle.publicationyear,
		srcFarEncyclopediaarticle.publisher,
		srcFarEncyclopediaarticle.webaddress,
		srcFarEncyclopediaarticle.additionalinfo,
		srcFarEncyclopediaarticle.dtcreated,
		srcFarEncyclopediaarticle.dtupdated,
		srcFarEncyclopediaarticle.userlastmodified,
		srcFarEncyclopediaarticle.ispublic,
		srcFarEncyclopediaarticle.activityid,
		srcFarEncyclopediaarticle.load_error,
		srcFarEncyclopediaarticle.data_origin,
		srcFarEncyclopediaarticle.created_ew_dttm,
		srcFarEncyclopediaarticle.lastupd_dw_dttm,
		srcFarEncyclopediaarticle.batch_sid ]

	srcHash = hashThisList( farEncyclopediaarticleList )

	def farEncyclopediaarticleExists():
		"""
			determine the farEncyclopediaarticle exists in the target database.
			@True: The farEncyclopediaarticle exists in the database
			@False: The farEncyclopediaarticle does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarEncyclopediaarticles.encyclopediaarticleid == srcFarEncyclopediaarticle.encyclopediaarticleid ) )

		return ret

	if farEncyclopediaarticleExists():

		def farEncyclopediaarticleUpdateRequired():
			"""
				Determine if the farEncyclopediaarticle that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarEncyclopediaarticles.encyclopediaarticleid == srcFarEncyclopediaarticle.encyclopediaarticleid ).where(
					FarEncyclopediaarticles.source_hash == srcHash ).where(	
					FarEncyclopediaarticles.deleted_at.is_( None ) ) )

			return not ret

		if farEncyclopediaarticleUpdateRequired():
			# retrive the tables object to update.
			updateFarEncyclopediaarticle = sesTarget.query(
				FarEncyclopediaarticles ).filter(
					FarEncyclopediaarticles.encyclopediaarticleid == srcFarEncyclopediaarticle.encyclopediaarticleid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarEncyclopediaarticle.source_hash = srcFarEncyclopediaarticle.source_hash
			updateFarEncyclopediaarticle.encyclopediaarticleid = srcFarEncyclopediaarticle.encyclopediaarticleid
			updateFarEncyclopediaarticle.src_sys_id = srcFarEncyclopediaarticle.src_sys_id
			updateFarEncyclopediaarticle.evaluationid = srcFarEncyclopediaarticle.evaluationid
			updateFarEncyclopediaarticle.authors = srcFarEncyclopediaarticle.authors
			updateFarEncyclopediaarticle.title = srcFarEncyclopediaarticle.title
			updateFarEncyclopediaarticle.publicationname = srcFarEncyclopediaarticle.publicationname
			updateFarEncyclopediaarticle.publicationstatuscode = srcFarEncyclopediaarticle.publicationstatuscode
			updateFarEncyclopediaarticle.pages = srcFarEncyclopediaarticle.pages
			updateFarEncyclopediaarticle.publicationyear = srcFarEncyclopediaarticle.publicationyear
			updateFarEncyclopediaarticle.publisher = srcFarEncyclopediaarticle.publisher
			updateFarEncyclopediaarticle.webaddress = srcFarEncyclopediaarticle.webaddress
			updateFarEncyclopediaarticle.additionalinfo = srcFarEncyclopediaarticle.additionalinfo
			updateFarEncyclopediaarticle.dtcreated = srcFarEncyclopediaarticle.dtcreated
			updateFarEncyclopediaarticle.dtupdated = srcFarEncyclopediaarticle.dtupdated
			updateFarEncyclopediaarticle.userlastmodified = srcFarEncyclopediaarticle.userlastmodified
			updateFarEncyclopediaarticle.ispublic = srcFarEncyclopediaarticle.ispublic
			updateFarEncyclopediaarticle.activityid = srcFarEncyclopediaarticle.activityid
			updateFarEncyclopediaarticle.load_error = srcFarEncyclopediaarticle.load_error
			updateFarEncyclopediaarticle.data_origin = srcFarEncyclopediaarticle.data_origin
			updateFarEncyclopediaarticle.created_ew_dttm = srcFarEncyclopediaarticle.created_ew_dttm
			updateFarEncyclopediaarticle.lastupd_dw_dttm = srcFarEncyclopediaarticle.lastupd_dw_dttm
			updateFarEncyclopediaarticle.batch_sid = srcFarEncyclopediaarticle.batch_sid
			updateFarEncyclopediaarticle.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarEncyclopediaarticle.deleted_at = None

			return updateFarEncyclopediaarticle
		else:
			raise TypeError('source farEncyclopediaarticle already exists and requires no updates!')

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarEncyclopediaarticle.evaluationid ).one()

		insertFarEncyclopediaarticle = FarEncyclopediaarticles(
			source_hash = srcFarEncyclopediaarticle.source_hash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			encyclopediaarticleid = srcFarEncyclopediaarticle.encyclopediaarticleid,
			src_sys_id = srcFarEncyclopediaarticle.src_sys_id,
			evaluationid = srcFarEncyclopediaarticle.evaluationid,
			authors = srcFarEncyclopediaarticle.authors,
			title = srcFarEncyclopediaarticle.title,
			publicationname = srcFarEncyclopediaarticle.publicationname,
			publicationstatuscode = srcFarEncyclopediaarticle.publicationstatuscode,
			pages = srcFarEncyclopediaarticle.pages,
			publicationyear = srcFarEncyclopediaarticle.publicationyear,
			publisher = srcFarEncyclopediaarticle.publisher,
			webaddress = srcFarEncyclopediaarticle.webaddress,
			additionalinfo = srcFarEncyclopediaarticle.additionalinfo,
			dtcreated = srcFarEncyclopediaarticle.dtcreated,
			dtupdated = srcFarEncyclopediaarticle.dtupdated,
			userlastmodified = srcFarEncyclopediaarticle.userlastmodified,
			ispublic = srcFarEncyclopediaarticle.ispublic,
			activityid = srcFarEncyclopediaarticle.activityid,
			load_error = srcFarEncyclopediaarticle.load_error,
			data_origin = srcFarEncyclopediaarticle.data_origin,
			created_ew_dttm = srcFarEncyclopediaarticle.created_ew_dttm,
			lastupd_dw_dttm = srcFarEncyclopediaarticle.lastupd_dw_dttm,
			batch_sid = srcFarEncyclopediaarticle.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarEncyclopediaarticle

def getTargetFarEncyclopediaarticles( sesTarget ):
	"""
		Returns a set of FarEncyclopediaarticles objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		FarEncyclopediaarticles ).filter(
			FarEncyclopediaarticles.deleted_at.is_( None ) ).all()

def softDeleteFarEncyclopediaarticle( tgtRecord, srcRecords ):
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
		return not any( srcRecord.encyclopediaarticleid == tgtRecord.encyclopediaarticleid for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')


