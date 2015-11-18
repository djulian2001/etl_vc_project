import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import Jobs, People, Departments
from asutobio.models.biopsmodels import BioPsJobs

#template mapping... plural Jobs    singularCaped Job   singularLower job

def getSourcePersonJobs( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the Jobs table of the source database.
	"""

	return sesSource.query( BioPsJobs ).all()

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

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

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
				Jobs.deptid == srcJob.deptid ) )

		return ret

	if jobExists():

		def jobUpdateRequired():
			"""
				Determine if the job that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					Jobs.emplid == srcJob.emplid ).where(
					Jobs.title == srcJob.title ).where(
					Jobs.deptid == srcJob.deptid ).where(
					Jobs.updated_flag == False).where(
					Jobs.source_hash != srcJob.source_hash ) )

			return ret

		if jobUpdateRequired():
			# retrive the tables object to update.
			updateJob = sesTarget.query(
				Jobs ).filter(
					Jobs.emplid == srcJob.emplid ).filter(
					Jobs.title == srcJob.title ).filter(
					Jobs.deptid == srcJob.deptid ).filter(
					Jobs.updated_flag == False ).first()

			# repeat the following pattern for all mapped attributes:
			updateJob.source_hash = srcJob.source_hash
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
			raise TypeError('source job already exists and requires no updates!')

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
			source_hash = srcJob.source_hash,
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

def softDeletePersonJob( tgtMissingJob, sesSource ):
	"""
		The list of Jobs changes as time moves on, the Jobs removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagPersonJobMissing( emplid, title, deptid ):
		"""
			Determine that the job object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsJobs.emplid == emplid ).where(
				BioPsJobs.title == title ).where(
				BioPsJobs.deptid == deptid ) )

		return not ret

	if flagPersonJobMissing( tgtMissingJob.emplid, tgtMissingJob.title, tgtMissingJob.deptid ):

		tgtMissingJob.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingJob

	else:
		raise TypeError('source person still exists and requires no soft delete!')
