import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import PersonSubAffiliations, Departments, People, SubAffiliations
from asutobio.models.biopsmodels import BioPsSubAffiliations

#template mapping... plural PersonSubAffiliations    singularCaped PersonSubAffiliation   singularLower personSubAffiliation

def getSourcePersonSubAffiliations( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the PersonSubAffiliations table of the source database.
	"""

	return sesSource.query( BioPsSubAffiliations ).all()

# change value to the singular
def processPersonSubAffiliation( srcPersonSubAffiliation, sesTarget ):
	"""
		Takes in a source PersonSubAffiliation object from biopsmodels (mysql.bio_ps.PersonSubAffiliations)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.PersonSubAffiliations), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

	def personSubAffiliationExists():
		"""
			determine the personSubAffiliation exists in the target database.
			@True: The personSubAffiliation exists in the database
			@False: The personSubAffiliation does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				PersonSubAffiliations.emplid == srcPersonSubAffiliation.emplid ).where(
				PersonSubAffiliations.deptid == srcPersonSubAffiliation.deptid ).where(
				PersonSubAffiliations.subaffiliation_code == srcPersonSubAffiliation.subaffiliation_code ) )

		return ret

	if personSubAffiliationExists():

		def personSubAffiliationUpdateRequired():
			"""
				Determine if the personSubAffiliation that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					PersonSubAffiliations.emplid == srcPersonSubAffiliation.emplid ).where(
					PersonSubAffiliations.deptid == srcPersonSubAffiliation.deptid ).where(
					PersonSubAffiliations.subaffiliation_code == srcPersonSubAffiliation.subaffiliation_code ).where(
					PersonSubAffiliations.source_hash == srcPersonSubAffiliation.source_hash ) )

			return not ret

		if personSubAffiliationUpdateRequired():
			# retrive the tables object to update.
			updatePersonSubAffiliation = sesTarget.query(
				PersonSubAffiliations ).filter(
					PersonSubAffiliations.emplid == srcPersonSubAffiliation.emplid ).filter(
					PersonSubAffiliations.deptid == srcPersonSubAffiliation.deptid ).filter(
					PersonSubAffiliations.subaffiliation_code == srcPersonSubAffiliation.subaffiliation_code ).one()

			# repeat the following pattern for all mapped attributes:
			updatePersonSubAffiliation.source_hash = srcPersonSubAffiliation.source_hash
			updatePersonSubAffiliation.updated_flag = True
			updatePersonSubAffiliation.emplid = srcPersonSubAffiliation.emplid
			updatePersonSubAffiliation.deptid = srcPersonSubAffiliation.deptid
			updatePersonSubAffiliation.subaffiliation_code = srcPersonSubAffiliation.subaffiliation_code
			updatePersonSubAffiliation.campus = srcPersonSubAffiliation.campus
			updatePersonSubAffiliation.title = srcPersonSubAffiliation.title
			updatePersonSubAffiliation.short_description = srcPersonSubAffiliation.short_description
			updatePersonSubAffiliation.description = srcPersonSubAffiliation.description
			updatePersonSubAffiliation.directory_publish = srcPersonSubAffiliation.directory_publish
			updatePersonSubAffiliation.department = srcPersonSubAffiliation.department
			updatePersonSubAffiliation.department_directory = srcPersonSubAffiliation.department_directory
			updatePersonSubAffiliation.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updatePersonSubAffiliation.deleted_at = None

			return updatePersonSubAffiliation
		else:
			raise TypeError('source personSubAffiliation already exists and requires no updates!')

	else:

		srcGetPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcPersonSubAffiliation.emplid ).one()

		srcGetDepartmentId = sesTarget.query(
			Departments.id ).filter(
				Departments.deptid == srcPersonSubAffiliation.deptid ).one()

		if srcGetPersonId and srcGetDepartmentId:
			
			srcGetSubAffiliationId = sesTarget.query(
				SubAffiliations.id ).filter(
					SubAffiliations.code == srcPersonSubAffiliation.subaffiliation_code ).first()

			if srcGetSubAffiliationId:
				srcSubaffiliationId = srcGetSubAffiliationId.id
			else:
				srcSubaffiliationId = None

			insertPersonSubAffiliation = PersonSubAffiliations(
				person_id = srcGetPersonId.id,
				department_id = srcGetDepartmentId.id,
				subaffiliation_id = srcSubaffiliationId,
				source_hash = srcPersonSubAffiliation.source_hash,
				emplid = srcPersonSubAffiliation.emplid,
				deptid = srcPersonSubAffiliation.deptid,
				subaffiliation_code = srcPersonSubAffiliation.subaffiliation_code,
				campus = srcPersonSubAffiliation.campus,
				title = srcPersonSubAffiliation.title,
				short_description = srcPersonSubAffiliation.short_description,
				description = srcPersonSubAffiliation.description,
				directory_publish = srcPersonSubAffiliation.directory_publish,
				department = srcPersonSubAffiliation.department,
				department_directory = srcPersonSubAffiliation.department_directory,
				created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

			return insertPersonSubAffiliation

		else:
			raise RuntimeError('source person(%s) or department(%s) does not exist within People and or Departments!' % ( srcPersonSubAffiliation.emplid, srcPersonSubAffiliation.deptid ))


def getTargetPersonSubAffiliations( sesTarget ):
	"""
		Returns a set of PersonSubAffiliations objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		PersonSubAffiliations ).filter(
			PersonSubAffiliations.deleted_at.is_( None ) ).all()

def softDeletePersonSubAffiliation( tgtMissingPersonSubAffiliation, sesSource ):
	"""
		The list of PersonSubAffiliations changes as time moves on, the PersonSubAffiliations removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagPersonSubAffiliationMissing():
		"""
			Determine that the personSubAffiliation object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsSubAffiliations.emplid == tgtMissingPersonSubAffiliation.emplid ).where(
				BioPsSubAffiliations.deptid == tgtMissingPersonSubAffiliation.deptid ).where(
				BioPsSubAffiliations.subaffiliation_code == tgtMissingPersonSubAffiliation.subaffiliation_code ) )

		return not ret

	if flagPersonSubAffiliationMissing():

		tgtMissingPersonSubAffiliation.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingPersonSubAffiliation

	else:
		raise TypeError('source person still exists and requires no soft delete!')


