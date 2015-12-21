import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import Jobs, People, Departments
from models.asudwpsmodels import AsuDwPsJobs, AsuPsBioFilters

def getSourcePersonJobs( sesSource ):
	"""
		Selects the data from the data wharehouse for the Jobs model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsJobs ).join(
			srcEmplidsSubQry, AsuDwPsJobs.emplid==srcEmplidsSubQry.c.emplid ).order_by(
				AsuDwPsJobs.emplid ).all()

# change value to the singular
def processPersonJob( srcJob, sesTarget ):
	"""
		Takes in a source Job object from biopsmodels (mysql.bio_ps.Jobs)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.Jobs), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

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

	def jobExists():
		"""
			determine the job exists in the target database.
			@True: The job exists in the database
			@False: The job does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				Jobs.emplid == srcJob.emplid ).where(
				Jobs.title == srcJob.title ).where(
				Jobs.deptid == srcJob.deptid ).where(
				Jobs.updated_flag == False ) )

		return ret

	if jobExists():
		def jobUpdates():
			"""
				Determine if the job that exists requires and update.
				@returns: returns the first record that matches the conditions.
			"""
			return sesTarget.query(
				Jobs ).filter(
					Jobs.emplid == srcJob.emplid ).filter(
					Jobs.title == srcJob.title ).filter(
					Jobs.deptid == srcJob.deptid ).filter(
					Jobs.updated_flag == False ).all()

		updateJobs = jobUpdates()

		for updateJob in updateJobs:
			if updateJob.source_hash == srcHash:
				
				updateJob.updated_flag = True
				updateJob.deleted_at = None
				return updateJob
				break

		else: # no break reached
			updateJob = updateJobs[0]

			# repeat the following pattern for all mapped attributes:
			updateJob.source_hash = srcHash
			updateJob.updated_flag = True
			updateJob.emplid = srcJob.emplid
			updateJob.empl_rcd = srcJob.empl_rcd
			updateJob.title = srcJob.title
			updateJob.department = srcJob.department
			updateJob.mailcode = srcJob.mailcode
			updateJob.empl_class = srcJob.empl_class
			updateJob.job_indicator = srcJob.job_indicator
			updateJob.location = srcJob.location
			updateJob.hr_status = srcJob.hr_status
			updateJob.deptid = srcJob.deptid
			updateJob.empl_status = srcJob.empl_status
			updateJob.fte = srcJob.fte
			updateJob.department_directory = srcJob.department_directory
			updateJob.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateJob.deleted_at = None
			
			return updateJob

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


def getTargetPersonJobs( sesTarget ):
	"""
		Returns a set of Jobs objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		Jobs ).filter(
			Jobs.deleted_at.is_( None ) ).all()


def softDeletePersonJob( tgtRecord, srcRecords ):
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
			srcRecord.deptid == tgtRecord.deptid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')
