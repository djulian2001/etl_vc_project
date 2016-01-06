import datetime
from sqlalchemy import exists, literal, func

from sharedProcesses import hashThisList
from models.biopublicmodels import JobCodes
from models.asudwpsmodels import AsuDwPsJobCodes

def getSourceJobCodes( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the JobCodes table of the source database.
	"""
	subJobCodes = (
        sesSource.query(
        	AsuDwPsJobCodes,
            func.row_number().over(
                partition_by=[AsuDwPsJobCodes.jobcode],
                order_by=AsuDwPsJobCodes.effdt.desc()
                ).label( 'rn' ) ) ).subquery()

	return sesSource.query(
		subJobCodes ).filter(
			subJobCodes.c.rn == 1 ).order_by(
				subJobCodes.c.jobcode ).all()

# change value to the singular
def processJob( srcJob, sesTarget ):
	"""
		Takes in a source Job object from biopsmodels (mysql.bio_ps.JobCodes)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.JobCodes), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	jobList = [
		srcJob.jobcode,
		srcJob.effdt,
		srcJob.setid,
		srcJob.src_sys_id,
		srcJob.eff_status,
		srcJob.descr,
		srcJob.descrshort,
		srcJob.setid_salary,
		srcJob.sal_admin_plan,
		srcJob.grade,
		srcJob.manager_level,
		srcJob.job_family,
		srcJob.flsa_status ]

	srcHash = hashThisList( jobList )

	def jobExists():
		"""
			determine the job exists in the target database.
			@True: The job exists in the database
			@False: The job does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				JobCodes.jobcode == srcJob.jobcode ) )

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
					JobCodes.jobcode == srcJob.jobcode ).where(
					JobCodes.source_hash == srcHash ).where(
					JobCodes.deleted_at.is_( None ) ) )

			return not ret

		if jobUpdateRequired():
			# retrive the tables object to update.
			updateJob = sesTarget.query(
				JobCodes ).filter(
					JobCodes.jobcode == srcJob.jobcode ).one()

			# repeat the following pattern for all mapped attributes:
			updateJob.source_hash = srcHash
			updateJob.jobcode = srcJob.jobcode
			updateJob.effdt = srcJob.effdt
			updateJob.setid = srcJob.setid
			updateJob.src_sys_id = srcJob.src_sys_id
			updateJob.eff_status = srcJob.eff_status
			updateJob.descr = srcJob.descr
			updateJob.descrshort = srcJob.descrshort
			updateJob.setid_salary = srcJob.setid_salary
			updateJob.sal_admin_plan = srcJob.sal_admin_plan
			updateJob.grade = srcJob.grade
			updateJob.manager_level = srcJob.manager_level
			updateJob.job_family = srcJob.job_family
			updateJob.flsa_status = srcJob.flsa_status
			updateJob.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateJob.deleted_at = None

			return updateJob

	else:
		insertJob = JobCodes(
			source_hash = srcHash,
			jobcode = srcJob.jobcode,
			effdt = srcJob.effdt,
			setid = srcJob.setid,
			src_sys_id = srcJob.src_sys_id,
			eff_status = srcJob.eff_status,
			descr = srcJob.descr,
			descrshort = srcJob.descrshort,
			setid_salary = srcJob.setid_salary,
			sal_admin_plan = srcJob.sal_admin_plan,
			grade = srcJob.grade,
			manager_level = srcJob.manager_level,
			job_family = srcJob.job_family,
			flsa_status = srcJob.flsa_status,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertJob

def getTargetJobCodes( sesTarget ):
	"""
		Returns a set of JobCodes objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		JobCodes ).filter(
			JobCodes.deleted_at.is_( None ) ).all()

def softDeleteJob( tgtRecord, srcRecords ):
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
		return not any( srcRecord.jobcode == tgtRecord.jobcode for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord

