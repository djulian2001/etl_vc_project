import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import FarConferenceProceedings, FarEvaluations
from asutobio.models.biopsmodels import BioPsFarConferenceProceedings


def getSourceFarConferenceProceedings( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarConferenceProceedings table of the source database.
	"""

	return sesSource.query( BioPsFarConferenceProceedings ).all()

# change value to the singular
def processFarConferenceProceeding( srcFarConferenceProceeding, sesTarget ):
	"""
		Takes in a source FarConferenceProceeding object from biopsmodels (mysql.bio_ps.FarConferenceProceedings)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarConferenceProceedings), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

#template mapping... column where(s) conferenceproceedingid 
	true, false = literal(True), literal(False)

	def farConferenceProceedingExists():
		"""
			determine the farConferenceProceeding exists in the target database.
			@True: The farConferenceProceeding exists in the database
			@False: The farConferenceProceeding does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarConferenceProceedings.conferenceproceedingid == srcFarConferenceProceeding.conferenceproceedingid ) )

		return ret

	if farConferenceProceedingExists():

		def farConferenceProceedingUpdateRequired():
			"""
				Determine if the farConferenceProceeding that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarConferenceProceedings.conferenceproceedingid == srcFarConferenceProceeding.conferenceproceedingid ).where(
					FarConferenceProceedings.source_hash == srcFarConferenceProceeding.source_hash ) )

			return not ret

		if farConferenceProceedingUpdateRequired():
			# retrive the tables object to update.
			updateFarConferenceProceeding = sesTarget.query(
				FarConferenceProceedings ).filter(
					FarConferenceProceedings.conferenceproceedingid == srcFarConferenceProceeding.conferenceproceedingid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarConferenceProceeding.source_hash = srcFarConferenceProceeding.source_hash
			updateFarConferenceProceeding.conferenceproceedingid = srcFarConferenceProceeding.conferenceproceedingid
			updateFarConferenceProceeding.src_sys_id = srcFarConferenceProceeding.src_sys_id
			updateFarConferenceProceeding.evaluationid = srcFarConferenceProceeding.evaluationid
			updateFarConferenceProceeding.authors = srcFarConferenceProceeding.authors
			updateFarConferenceProceeding.title = srcFarConferenceProceeding.title
			updateFarConferenceProceeding.journalname = srcFarConferenceProceeding.journalname
			updateFarConferenceProceeding.refereed = srcFarConferenceProceeding.refereed
			updateFarConferenceProceeding.publicationstatuscode = srcFarConferenceProceeding.publicationstatuscode
			updateFarConferenceProceeding.publicationyear = srcFarConferenceProceeding.publicationyear
			updateFarConferenceProceeding.volumenumber = srcFarConferenceProceeding.volumenumber
			updateFarConferenceProceeding.pages = srcFarConferenceProceeding.pages
			updateFarConferenceProceeding.webaddress = srcFarConferenceProceeding.webaddress
			updateFarConferenceProceeding.abstract = srcFarConferenceProceeding.abstract
			updateFarConferenceProceeding.additionalinfo = srcFarConferenceProceeding.additionalinfo
			updateFarConferenceProceeding.dtcreated = srcFarConferenceProceeding.dtcreated
			updateFarConferenceProceeding.dtupdated = srcFarConferenceProceeding.dtupdated
			updateFarConferenceProceeding.userlastmodified = srcFarConferenceProceeding.userlastmodified
			updateFarConferenceProceeding.ispublic = srcFarConferenceProceeding.ispublic
			updateFarConferenceProceeding.activityid = srcFarConferenceProceeding.activityid
			updateFarConferenceProceeding.load_error = srcFarConferenceProceeding.load_error
			updateFarConferenceProceeding.data_origin = srcFarConferenceProceeding.data_origin
			updateFarConferenceProceeding.created_ew_dttm = srcFarConferenceProceeding.created_ew_dttm
			updateFarConferenceProceeding.lastupd_dw_dttm = srcFarConferenceProceeding.lastupd_dw_dttm
			updateFarConferenceProceeding.batch_sid = srcFarConferenceProceeding.batch_sid
			updateFarConferenceProceeding.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarConferenceProceeding.deleted_at = None

			return updateFarConferenceProceeding
		else:
			raise TypeError('source farConferenceProceeding already exists and requires no updates!')

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarConferenceProceeding.evaluationid ).one()

		insertFarConferenceProceeding = FarConferenceProceedings(
			source_hash = srcFarConferenceProceeding.source_hash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			conferenceproceedingid = srcFarConferenceProceeding.conferenceproceedingid,
			src_sys_id = srcFarConferenceProceeding.src_sys_id,
			evaluationid = srcFarConferenceProceeding.evaluationid,
			authors = srcFarConferenceProceeding.authors,
			title = srcFarConferenceProceeding.title,
			journalname = srcFarConferenceProceeding.journalname,
			refereed = srcFarConferenceProceeding.refereed,
			publicationstatuscode = srcFarConferenceProceeding.publicationstatuscode,
			publicationyear = srcFarConferenceProceeding.publicationyear,
			volumenumber = srcFarConferenceProceeding.volumenumber,
			pages = srcFarConferenceProceeding.pages,
			webaddress = srcFarConferenceProceeding.webaddress,
			abstract = srcFarConferenceProceeding.abstract,
			additionalinfo = srcFarConferenceProceeding.additionalinfo,
			dtcreated = srcFarConferenceProceeding.dtcreated,
			dtupdated = srcFarConferenceProceeding.dtupdated,
			userlastmodified = srcFarConferenceProceeding.userlastmodified,
			ispublic = srcFarConferenceProceeding.ispublic,
			activityid = srcFarConferenceProceeding.activityid,
			load_error = srcFarConferenceProceeding.load_error,
			data_origin = srcFarConferenceProceeding.data_origin,
			created_ew_dttm = srcFarConferenceProceeding.created_ew_dttm,
			lastupd_dw_dttm = srcFarConferenceProceeding.lastupd_dw_dttm,
			batch_sid = srcFarConferenceProceeding.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarConferenceProceeding

def getTargetFarConferenceProceedings( sesTarget ):
	"""
		Returns a set of FarConferenceProceedings objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		FarConferenceProceedings ).filter(
			FarConferenceProceedings.deleted_at.is_( None ) ).all()

def softDeleteFarConferenceProceeding( tgtMissingFarConferenceProceeding, sesSource ):
	"""
		The list of FarConferenceProceedings changes as time moves on, the FarConferenceProceedings removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagFarConferenceProceedingMissing():
		"""
			Determine that the farConferenceProceeding object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsFarConferenceProceedings.conferenceproceedingid == tgtMissingFarConferenceProceeding.conferenceproceedingid ) )

		return not ret

	if flagFarConferenceProceedingMissing():

		tgtMissingFarConferenceProceeding.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingFarConferenceProceeding

	else:
		raise TypeError('source person still exists and requires no soft delete!')

