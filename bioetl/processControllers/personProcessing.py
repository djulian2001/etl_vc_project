import datetime

from bioetl.sharedProcesses import hashThisList, AsuPsBioFilters
from models.biopublicmodels import People
from models.asudwpsmodels import AsuDwPsPerson

def getTableName():
	return People.__table__.name

def getSourceData( sesSource, appState=None, qryList=None ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of person records from the people table of the source database.
	"""

	if not qryList:
		srcFilters = AsuPsBioFilters( sesSource, appState.subAffCodes )

		srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

		return sesSource.query( 
			AsuDwPsPerson ).join(
				srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid ).order_by(
					AsuDwPsPerson.emplid ).all()
	else:
		return sesSource.query( 
			AsuDwPsPerson ).filter(
                AsuDwPsPerson.emplid.in_( qryList ) ).all()

def processData( srcPerson, sesTarget ):
	"""
		Takes in a source person object from the asudw as a AsuDwPsPerson object
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.people), or that nothing needs doing, but each
		source record will have an action in the target database via the
		updated_flag.
	"""

	personList = [
		srcPerson.emplid,
		srcPerson.asurite_id,
		srcPerson.asu_id,
		srcPerson.ferpa,
		srcPerson.last_name,
		srcPerson.first_name,
		srcPerson.middle_name,
		srcPerson.display_name,
		srcPerson.preferred_first_name,
		srcPerson.affiliations,
		srcPerson.email_address,
		srcPerson.eid,
		srcPerson.birthdate,
		srcPerson.last_update ]

	srcHash = hashThisList( personList )

  	def getTargetRecords():
		"""Returns a record set from the target database."""
		"""
			Determine the person exists in the target database.
			@True: The person exists in the database
			@False: The person does not exist in the database

		"""
		ret = sesTarget.query(
			People ).filter(
				People.emplid == srcPerson.emplid ).all()
		
		return ret

	tgtRecords = getTargetRecords()

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
				return tgtRecord
				break

		else: # NO BREAK REACHED
			tgtRecord = tgtRecords[0]

			tgtRecord.source_hash = srcHash
			tgtRecord.updated_flag = True
			tgtRecord.emplid = srcPerson.emplid
			tgtRecord.asurite_id = srcPerson.asurite_id
			tgtRecord.asu_id = srcPerson.asu_id
			tgtRecord.ferpa = srcPerson.ferpa
			tgtRecord.last_name = srcPerson.last_name
			tgtRecord.first_name = srcPerson.first_name
			tgtRecord.middle_name = srcPerson.middle_name
			tgtRecord.display_name = srcPerson.display_name
			tgtRecord.preferred_first_name = srcPerson.preferred_first_name
			tgtRecord.affiliations = srcPerson.affiliations
			tgtRecord.email_address = srcPerson.email_address
			tgtRecord.eid = srcPerson.eid
			tgtRecord.birthdate = srcPerson.birthdate
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None

			return tgtRecord 
	
	else:
		# person wasn't in the target databases, add them now
		
		if srcPerson.emplid >= 2147483647L:
			raise ValueError

		insertPerson = People(
			source_hash = srcHash,
			updated_flag = True,
			emplid = srcPerson.emplid,
			asurite_id = srcPerson.asurite_id,
			asu_id = srcPerson.asu_id,
			ferpa = srcPerson.ferpa,
			last_name = srcPerson.last_name,
			first_name = srcPerson.first_name,
			middle_name = srcPerson.middle_name,
			display_name = srcPerson.display_name,
			preferred_first_name = srcPerson.preferred_first_name,
			affiliations = srcPerson.affiliations,
			email_address = srcPerson.email_address,
			eid = srcPerson.eid,
			birthdate = srcPerson.birthdate,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )
		
		return insertPerson

def getTargetData( sesTarget ):
	"""
		Returns a set of People object from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
			People ).filter(
				People.deleted_at.is_( None ) ).all()


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
		return not any( srcRecord.emplid == tgtRecord.emplid for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
