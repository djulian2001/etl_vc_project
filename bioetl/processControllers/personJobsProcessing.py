import datetime

from bioetl.sharedProcesses import hashThisList, AsuPsBioFilters
from models.biopublicmodels import Jobs, People, Departments
from models.asudwpsmodels import AsuDwPsJobs

def getTableName():
	return Jobs.__table__.name

def getSourceData( sesSource, appState=None, qryList=None ):
	"""
		Selects the data from the data wharehouse for the Jobs model.
		@returns: the record set
	"""
	if not qryList:
		srcFilters = AsuPsBioFilters( sesSource, appState.subAffCodes )

		srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

		return sesSource.query(
			AsuDwPsJobs ).filter( AsuDwPsJobs.job_indicator=='P' ).join(
				srcEmplidsSubQry, AsuDwPsJobs.emplid==srcEmplidsSubQry.c.emplid ).order_by(
					AsuDwPsJobs.emplid ).all()
	else:
		return sesSource.query(
			AsuDwPsJobs ).filter(
				AsuDwPsJobs.emplid.in_( qryList ) ).all()


def processData( srcJob, sesTarget ):
	"""
		Takes in a source Job object from biopsmodels (mysql.bio_ps.Jobs)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.Jobs), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	recordToList = [
		srcJob.emplid,
		srcJob.empl_rcd,
		srcJob.title,
		srcJob.department,
		srcJob.mailcode,
		srcJob.empl_class,
		srcJob.job_indicator,
		srcJob.location,
		srcJob.hr_status,
		srcJob.deptid,
		srcJob.empl_status,
		srcJob.fte,
		srcJob.last_update,
		srcJob.department_directory ]

	srcHash = hashThisList( recordToList )

	def getTargetRecords():
		"""
			determine the job exists in the target database.
			@True: The job exists in the database
			@False: The job does not exist in the database
		"""
		ret = sesTarget.query(
			Jobs ).filter(
				Jobs.emplid == srcJob.emplid ).filter(
				Jobs.title == srcJob.title ).filter(
				Jobs.deptid == srcJob.deptid ).filter(
				Jobs.job_indicator == srcJob.job_indicator).filter(
				Jobs.updated_flag == False ).all()

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
			# repeat the following pattern for all mapped attributes:
			tgtRecord.source_hash = srcHash
			tgtRecord.updated_flag = True
			tgtRecord.emplid = srcJob.emplid
			tgtRecord.empl_rcd = srcJob.empl_rcd
			tgtRecord.title = srcJob.title
			tgtRecord.department = srcJob.department
			tgtRecord.mailcode = srcJob.mailcode
			tgtRecord.empl_class = srcJob.empl_class
			tgtRecord.job_indicator = srcJob.job_indicator
			tgtRecord.location = srcJob.location
			tgtRecord.hr_status = srcJob.hr_status
			tgtRecord.deptid = srcJob.deptid
			tgtRecord.empl_status = srcJob.empl_status
			tgtRecord.fte = srcJob.fte
			tgtRecord.department_directory = srcJob.department_directory
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None
			
			return tgtRecord

	else:
		# get the ids required to maintain relationships 
		# THIS IS WHERE WE WOULD CATCH Mid Pull new people....
		srcGetPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcJob.emplid ).one()

		srcGetDepartmentId = sesTarget.query(
			Departments.id ).filter(
				Departments.deptid == srcJob.deptid ).one()

		insertJob = Jobs(
			person_id = srcGetPersonId.id,
			department_id = srcGetDepartmentId.id,
			updated_flag = True,
			source_hash = srcHash,
			emplid = srcJob.emplid,
			empl_rcd = srcJob.empl_rcd,
			title = srcJob.title,
			department = srcJob.department,
			mailcode = srcJob.mailcode,
			empl_class = srcJob.empl_class,
			job_indicator = srcJob.job_indicator,
			location = srcJob.location,
			hr_status = srcJob.hr_status,
			deptid = srcJob.deptid,
			empl_status = srcJob.empl_status,
			fte = srcJob.fte,
			department_directory = srcJob.department_directory,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertJob


def getTargetData( sesTarget ):
	"""
		Returns a set of Jobs objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		Jobs ).filter(
			Jobs.deleted_at.is_( None ) ).all()


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
		return not any(
			srcRecord.emplid == tgtRecord.emplid and
			srcRecord.title == tgtRecord.title and
			srcRecord.deptid == tgtRecord.deptid and 
			srcRecord.job_indicator == tgtRecord.job_indicator for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
