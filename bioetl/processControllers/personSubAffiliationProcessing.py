import datetime

from bioetl.sharedProcesses import hashThisList, AsuPsBioFilters
from models.biopublicmodels import PersonSubAffiliations, Departments, People, SubAffiliations
from models.asudwpsmodels import AsuDwPsSubAffiliations

def getTableName():
	return PersonSubAffiliations.__table__.name


def getSourceData( sesSource, appState=None, qryList=None ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the PersonSubAffiliations table of the source database.
	"""
	if not qryList:
		srcFilters = AsuPsBioFilters( sesSource, appState.subAffCodes )

		srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

		return sesSource.query(
			AsuDwPsSubAffiliations ).join(
				srcEmplidsSubQry, AsuDwPsSubAffiliations.emplid==srcEmplidsSubQry.c.emplid ).order_by(
					AsuDwPsSubAffiliations.emplid ).all()
	else:
		return sesSource.query(
				AsuDwPsSubAffiliations ).filter(
					AsuDwPsSubAffiliations.emplid.in_( qryList ) ).all()

def processData( srcPersonSubAffiliation, sesTarget ):
	"""
		Takes in a source PersonSubAffiliation object from biopsmodels (mysql.bio_ps.PersonSubAffiliations)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.PersonSubAffiliations), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
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

	def getSubAffiliations():
		return sesTarget.query(
				SubAffiliations.id, SubAffiliations.display_title ).filter(
					SubAffiliations.code == srcPersonSubAffiliation.subaffiliation_code ).first()
	
	def getTargetRecords():
		"""
			determine the personSubAffiliation exists in the target database.
			@True: The personSubAffiliation exists in the database
			@False: The personSubAffiliation does not exist in the database
		"""
		ret = sesTarget.query(
			PersonSubAffiliations ).filter(
				PersonSubAffiliations.emplid == srcPersonSubAffiliation.emplid ).filter(
				PersonSubAffiliations.deptid == srcPersonSubAffiliation.deptid ).filter(
				PersonSubAffiliations.subaffiliation_code == srcPersonSubAffiliation.subaffiliation_code ).filter(
				PersonSubAffiliations.updated_flag == False ).all()

		return ret

	tgtRecords = getTargetRecords()

	srcGetSubAffiliationId = getSubAffiliations()

	if srcGetSubAffiliationId:
		srcSubaffiliationId = srcGetSubAffiliationId.id
		srcSubaffiliationDisplayTitle = srcGetSubAffiliationId.display_title
	else:
		srcSubaffiliationId = None
		srcSubaffiliationDisplayTitle = srcPersonSubAffiliation.description

	if tgtRecords:
		""" 
			If true then an update is required, else an insert is required
			@True:
				Because there might be many recornds returned from the db, a loop is required.
				Trying not to update the data if it is not required, but the source data will
				require an action.
				@Else Block (NO BREAK REACHED):
					If the condition is not reached in the for block the else block 
					will insure	that a record is updated.  
					It might not update the record that	was initially created previously,
					but all source data has to be represented in the target database.
			@False:
				insert the new data from the source database.
		"""
		for tgtRecord in tgtRecords:

			if tgtRecord.source_hash == srcHash:
				tgtRecord.updated_flag = True
				tgtRecord.deleted_at = None
				tgtRecord.subaffiliation_id = srcSubaffiliationId
				tgtRecord.bio_override_description = srcSubaffiliationDisplayTitle
				return tgtRecord
				break

		else: # NO BREAK REACHED
			tgtRecord = tgtRecords[0]

			tgtRecord.source_hash = srcHash
			tgtRecord.updated_flag = True
			tgtRecord.emplid = srcPersonSubAffiliation.emplid
			tgtRecord.deptid = srcPersonSubAffiliation.deptid
			tgtRecord.subaffiliation_id = srcSubaffiliationId
			tgtRecord.bio_override_description = srcSubaffiliationDisplayTitle
			tgtRecord.subaffiliation_code = srcPersonSubAffiliation.subaffiliation_code
			tgtRecord.campus = srcPersonSubAffiliation.campus
			tgtRecord.title = srcPersonSubAffiliation.title
			tgtRecord.short_description = srcPersonSubAffiliation.short_description
			tgtRecord.description = srcPersonSubAffiliation.description
			tgtRecord.directory_publish = srcPersonSubAffiliation.directory_publish
			tgtRecord.department = srcPersonSubAffiliation.department
			tgtRecord.department_directory = srcPersonSubAffiliation.department_directory
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None

			return tgtRecord
	
	else:
		srcGetPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcPersonSubAffiliation.emplid ).one()

		srcGetDepartmentId = sesTarget.query(
			Departments.id ).filter(
				Departments.deptid == srcPersonSubAffiliation.deptid ).one()

		if srcGetPersonId and srcGetDepartmentId:
			
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
				bio_override_description = srcSubaffiliationDisplayTitle,
				description = srcPersonSubAffiliation.description,
				directory_publish = srcPersonSubAffiliation.directory_publish,
				department = srcPersonSubAffiliation.department,
				department_directory = srcPersonSubAffiliation.department_directory,
				created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

			return insertPersonSubAffiliation


def getTargetData( sesTarget ):
	"""
		Returns a set of PersonSubAffiliations objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		PersonSubAffiliations ).filter(
			PersonSubAffiliations.deleted_at.is_( None ) ).all()

def softDeleteData( tgtRecord, srcRecords ):
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
