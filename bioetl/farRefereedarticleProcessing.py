import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import FarRefereedarticles, FarEvaluations
from asutobio.models.biopsmodels import BioPsFarRefereedarticles


def getSourceFarRefereedarticles( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarRefereedarticles table of the source database.
	"""

	return sesSource.query( BioPsFarRefereedarticles ).all()

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

#template mapping... column where(s) refereedarticleid 
	true, false = literal(True), literal(False)

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
					FarRefereedarticles.source_hash == srcFarRefereedarticle.source_hash ) )

			return not ret

		if farRefereedarticleUpdateRequired():
			# retrive the tables object to update.
			updateFarRefereedarticle = sesTarget.query(
				FarRefereedarticles ).filter(
					FarRefereedarticles.refereedarticleid == srcFarRefereedarticle.refereedarticleid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarRefereedarticle.source_hash = srcFarRefereedarticle.source_hash
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
			raise TypeError('source farRefereedarticle already exists and requires no updates!')

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarRefereedarticle.evaluationid ).one()

		insertFarRefereedarticle = FarRefereedarticles(
			source_hash = srcFarRefereedarticle.source_hash,
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

def softDeleteFarRefereedarticle( tgtMissingFarRefereedarticle, sesSource ):
	"""
		The list of FarRefereedarticles changes as time moves on, the FarRefereedarticles removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagFarRefereedarticleMissing():
		"""
			Determine that the farRefereedarticle object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsFarRefereedarticles.refereedarticleid == tgtMissingFarRefereedarticle.refereedarticleid ) )

		return not ret

	if flagFarRefereedarticleMissing():

		tgtMissingFarRefereedarticle.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingFarRefereedarticle

	else:
		raise TypeError('source person still exists and requires no soft delete!')
