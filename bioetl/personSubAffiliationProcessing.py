import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import PersonSubAffiliations, Departments, People, SubAffiliations
from models.asudwpsmodels import AsuDwPsSubAffiliations, AsuPsBioFilters

#template mapping... plural PersonSubAffiliations    singularCaped PersonSubAffiliation   singularLower personSubAffiliation

def getSourcePersonSubAffiliations( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the PersonSubAffiliations table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsSubAffiliations ).join(
			srcEmplidsSubQry, AsuDwPsSubAffiliations.emplid==srcEmplidsSubQry.c.emplid ).order_by(
				AsuDwPsSubAffiliations.emplid ).all()


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
	true, false = literal(True), literal(False)

	recordToList = [
		srcPersonSubAffiliation.emplid,
		srcPersonSubAffiliation.deptid,
		srcPersonSubAffiliation.subaffiliation_code,
		srcPersonSubAffiliation.campus,
		srcPersonSubAffiliation.title,
		srcPersonSubAffiliation.short_description,
		srcPersonSubAffiliation.description,
		srcPersonSubAffiliation.directory_publish,
		srcPersonSubAffiliation.department,
		srcPersonSubAffiliation.last_update,
		srcPersonSubAffiliation.department_directory ]

	srcHash = hashThisList( recordToList )

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
				PersonSubAffiliations.subaffiliation_code == srcPersonSubAffiliation.subaffiliation_code ).where(
				PersonSubAffiliations.updated_flag == False) )

		return ret

	if personSubAffiliationExists():

		def personSubAffiliationUpdates():
			"""
				Determine if the personSubAffiliation that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""
			return sesTarget.query(
				PersonSubAffiliations ).filter(
					PersonSubAffiliations.emplid == srcPersonSubAffiliation.emplid ).filter(
					PersonSubAffiliations.deptid == srcPersonSubAffiliation.deptid ).filter(
					PersonSubAffiliations.subaffiliation_code == srcPersonSubAffiliation.subaffiliation_code ).filter(
					PersonSubAffiliations.updated_flag == False ).all()

		updateOneOfThese = personSubAffiliationUpdates()

		for updateThisOne in updateOneOfThese:
			if updateThisOne.source_hash == srcHash:
				
				updateThisOne.updated_flag = True
				updateThisOne.deleted_at = None
				
				return updateThisOne
				break
		else: # no break reached
			updateThisOne = updateOneOfThese[0]

			updateThisOne.source_hash = srcHash
			updateThisOne.updated_flag = True
			updateThisOne.emplid = srcPersonSubAffiliation.emplid
			updateThisOne.deptid = srcPersonSubAffiliation.deptid
			updateThisOne.subaffiliation_code = srcPersonSubAffiliation.subaffiliation_code
			updateThisOne.campus = srcPersonSubAffiliation.campus
			updateThisOne.title = srcPersonSubAffiliation.title
			updateThisOne.short_description = srcPersonSubAffiliation.short_description
			updateThisOne.description = srcPersonSubAffiliation.description
			updateThisOne.directory_publish = srcPersonSubAffiliation.directory_publish
			updateThisOne.department = srcPersonSubAffiliation.department
			updateThisOne.department_directory = srcPersonSubAffiliation.department_directory
			updateThisOne.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateThisOne.deleted_at = None

			return updateThisOne
	
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
				updated_flag = True,
				subaffiliation_id = srcSubaffiliationId,
				source_hash = srcHash,
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


def softDeletePersonSubAffiliation( tgtRecord, srcRecords ):
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
		return not any( srcRecord.emplid == tgtRecord.emplid and
						srcRecord.deptid == tgtRecord.deptid and
						srcRecord.subaffiliation_code == tgtRecord.subaffiliation_code for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')
