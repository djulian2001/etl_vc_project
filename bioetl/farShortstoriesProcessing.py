import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarShortstories, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarShortstories, AsuDwPsFarEvaluations, AsuPsBioFilters

def getSourceFarShortstories( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarShortstories table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarShortstories ).join(
			farEvals, AsuDwPsFarShortstories.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarShortstories.ispublic !='N' ).all()

# change value to the singular
def processFarShortstorie( srcFarShortstorie, sesTarget ):
	"""
		Takes in a source FarShortstorie object from biopsmodels (mysql.bio_ps.FarShortstories)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarShortstories), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farShortstoriesList = [
		farShortstories.shortstoryid,
		farShortstories.src_sys_id,
		farShortstories.evaluationid,
		farShortstories.authors,
		farShortstories.title,
		farShortstories.publicationname,
		farShortstories.publicationstatuscode,
		farShortstories.pages,
		farShortstories.publicationyear,
		farShortstories.publisher,
		farShortstories.webaddress,
		farShortstories.translated,
		farShortstories.additionalinfo,
		farShortstories.dtcreated,
		farShortstories.dtupdated,
		farShortstories.userlastmodified,
		farShortstories.ispublic,
		farShortstories.activityid,
		farShortstories.load_error,
		farShortstories.data_origin,
		farShortstories.created_ew_dttm,
		farShortstories.lastupd_dw_dttm,
		farShortstories.batch_sid ]

	srcHash = hashThisList( farShortstoriesList )

	def farShortstoriesExists():
		"""
			determine the farShortstories exists in the target database.
			@True: The farShortstories exists in the database
			@False: The farShortstories does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarShortstories.shortstoryid == srcFarShortstorie.shortstoryid ) )

		return ret

	if farShortstoriesExists():

		def farShortstoriesUpdateRequired():
			"""
				Determine if the farShortstories that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarShortstories.shortstoryid == srcFarShortstorie.shortstoryid ).where(
					FarShortstories.source_hash == srcHash ).where(	
					FarShortstories.deleted_at.is_( None ) ) )

			return not ret

		if farShortstoriesUpdateRequired():
			# retrive the tables object to update.
			updateFarShortstorie = sesTarget.query(
				FarShortstories ).filter(
					FarShortstories.shortstoryid == srcFarShortstorie.shortstoryid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarShortstorie.source_hash = srcHash
			updateFarShortstorie.shortstoryid = srcFarShortstorie.shortstoryid
			updateFarShortstorie.src_sys_id = srcFarShortstorie.src_sys_id
			updateFarShortstorie.evaluationid = srcFarShortstorie.evaluationid
			updateFarShortstorie.authors = srcFarShortstorie.authors
			updateFarShortstorie.title = srcFarShortstorie.title
			updateFarShortstorie.publicationname = srcFarShortstorie.publicationname
			updateFarShortstorie.publicationstatuscode = srcFarShortstorie.publicationstatuscode
			updateFarShortstorie.pages = srcFarShortstorie.pages
			updateFarShortstorie.publicationyear = srcFarShortstorie.publicationyear
			updateFarShortstorie.publisher = srcFarShortstorie.publisher
			updateFarShortstorie.webaddress = srcFarShortstorie.webaddress
			updateFarShortstorie.translated = srcFarShortstorie.translated
			updateFarShortstorie.additionalinfo = srcFarShortstorie.additionalinfo
			updateFarShortstorie.dtcreated = srcFarShortstorie.dtcreated
			updateFarShortstorie.dtupdated = srcFarShortstorie.dtupdated
			updateFarShortstorie.userlastmodified = srcFarShortstorie.userlastmodified
			updateFarShortstorie.ispublic = srcFarShortstorie.ispublic
			updateFarShortstorie.activityid = srcFarShortstorie.activityid
			updateFarShortstorie.load_error = srcFarShortstorie.load_error
			updateFarShortstorie.data_origin = srcFarShortstorie.data_origin
			updateFarShortstorie.created_ew_dttm = srcFarShortstorie.created_ew_dttm
			updateFarShortstorie.lastupd_dw_dttm = srcFarShortstorie.lastupd_dw_dttm
			updateFarShortstorie.batch_sid = srcFarShortstorie.batch_sid
			updateFarShortstorie.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarShortstorie.deleted_at = None

			return updateFarShortstorie
		else:
			raise TypeError('source farShortstories already exists and requires no updates!')

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarShortstorie.evaluationid ).one()

		insertFarShortstorie = FarShortstories(
			source_hash = srcHash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			shortstoryid = srcFarShortstorie.shortstoryid,
			src_sys_id = srcFarShortstorie.src_sys_id,
			evaluationid = srcFarShortstorie.evaluationid,
			authors = srcFarShortstorie.authors,
			title = srcFarShortstorie.title,
			publicationname = srcFarShortstorie.publicationname,
			publicationstatuscode = srcFarShortstorie.publicationstatuscode,
			pages = srcFarShortstorie.pages,
			publicationyear = srcFarShortstorie.publicationyear,
			publisher = srcFarShortstorie.publisher,
			webaddress = srcFarShortstorie.webaddress,
			translated = srcFarShortstorie.translated,
			additionalinfo = srcFarShortstorie.additionalinfo,
			dtcreated = srcFarShortstorie.dtcreated,
			dtupdated = srcFarShortstorie.dtupdated,
			userlastmodified = srcFarShortstorie.userlastmodified,
			ispublic = srcFarShortstorie.ispublic,
			activityid = srcFarShortstorie.activityid,
			load_error = srcFarShortstorie.load_error,
			data_origin = srcFarShortstorie.data_origin,
			created_ew_dttm = srcFarShortstorie.created_ew_dttm,
			lastupd_dw_dttm = srcFarShortstorie.lastupd_dw_dttm,
			batch_sid = srcFarShortstorie.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarShortstorie

def getTargetFarShortstories( sesTarget ):
	"""
		Returns a set of FarShortstories objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		FarShortstories ).filter(
			FarShortstories.deleted_at.is_( None ) ).all()

def softDeleteFarShortstorie( tgtRecord, srcRecords ):
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
		return not any( srcRecord.shortstoryid == tgtRecord.shortstoryid for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')


