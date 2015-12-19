import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import JobsLog, People, Departments, JobCodes
from models.asudwpsmodels import AsuDwPsJobsLog, AsuPsBioFilters

def getSourceJobsLog( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the JobsLog table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsJobsLog ).join(
			srcEmplidsSubQry, AsuDwPsJobsLog.emplid==srcEmplidsSubQry.c.emplid ).filter(
			AsuDwPsJobsLog.action.in_( ('HIR','REH','RET','TER','XFR') ) ).all()

# change value to the singular
def processJobLog( srcJobLog, sesTarget ):
	"""
		Takes in a source JobLog object from biopsmodels (mysql.bio_ps.JobsLog)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.JobsLog), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	jobLogList = [
		srcJobLog.emplid,
		srcJobLog.deptid,
		srcJobLog.jobcode,
		srcJobLog.supervisor_id,
		srcJobLog.main_appt_num_jpn,
		srcJobLog.effdt,
		srcJobLog.action,
		srcJobLog.action_reason,
		srcJobLog.action_dt,
		srcJobLog.job_entry_dt,
		srcJobLog.dept_entry_dt,
		srcJobLog.position_entry_dt,
		srcJobLog.hire_dt,
		srcJobLog.last_hire_dt,
		srcJobLog.termination_dt ]

	srcHash = hashThisList( jobLogList )

	def jobLogExists():
		"""
			determine the jobLog exists in the target database.
			@True: The jobLog exists in the database
			@False: The jobLog does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				JobsLog.emplid == srcJobLog.emplid ).where(
				JobsLog.deptid == srcJobLog.deptid ).where(
				JobsLog.jobcode == srcJobLog.jobcode ).where(
				JobsLog.effdt == srcJobLog.effdt ).where(
				JobsLog.action == srcJobLog.action ).where(
				JobsLog.action_reason == srcJobLog.action_reason ) )

		return ret

	if jobLogExists():

		def jobLogUpdateRequired():
			"""
				Determine if the jobLog that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					JobsLog.emplid == srcJobLog.emplid ).where(
					JobsLog.deptid == srcJobLog.deptid ).where(
					JobsLog.jobcode == srcJobLog.jobcode ).where(
					JobsLog.effdt == srcJobLog.effdt ).where(
					JobsLog.action == srcJobLog.action ).where(
					JobsLog.action_reason == srcJobLog.action_reason ).where(
					JobsLog.source_hash == srcHash ).where(
					JobsLog.deleted_at.is_( None ) ) )

			return not ret

		if jobLogUpdateRequired():
			# retrive the tables object to update.
			updateJobLog = sesTarget.query(
				JobsLog ).filter(
					JobsLog.emplid == srcJobLog.emplid ).filter(
					JobsLog.deptid == srcJobLog.deptid ).filter(
					JobsLog.jobcode == srcJobLog.jobcode ).filter(
					JobsLog.effdt == srcJobLog.effdt ).filter(
					JobsLog.action == srcJobLog.action ).filter(
					JobsLog.action_reason == srcJobLog.action_reason ).filter(
					JobsLog.deleted_at.is_( None ) ).one()

			# repeat the following pattern for all mapped attributes:
			updateJobLog.source_hash = srcHash
			updateJobLog.emplid = srcJobLog.emplid
			updateJobLog.deptid = srcJobLog.deptid
			updateJobLog.jobcode = srcJobLog.jobcode
			updateJobLog.supervisor_id = srcJobLog.supervisor_id
			updateJobLog.main_appt_num_jpn = srcJobLog.main_appt_num_jpn
			updateJobLog.effdt = srcJobLog.effdt
			updateJobLog.action = srcJobLog.action
			updateJobLog.action_reason = srcJobLog.action_reason
			updateJobLog.action_dt = srcJobLog.action_dt
			updateJobLog.job_entry_dt = srcJobLog.job_entry_dt
			updateJobLog.dept_entry_dt = srcJobLog.dept_entry_dt
			updateJobLog.position_entry_dt = srcJobLog.position_entry_dt
			updateJobLog.hire_dt = srcJobLog.hire_dt
			updateJobLog.last_hire_dt = srcJobLog.last_hire_dt
			updateJobLog.termination_dt = srcJobLog.termination_dt
			updateJobLog.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateJobLog.deleted_at = None

			return updateJobLog
		else:
			raise TypeError('source jobLog already exists and requires no updates!')

	else:
		srcGetPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcJobLog.emplid ).one()

		srcGetDepartmentId = sesTarget.query(
			Departments.id ).filter(
				Departments.deptid == srcJobLog.deptid ).one()

		srcGetJobId = sesTarget.query(
			JobCodes.id ).filter(
				JobCodes.jobcode == srcJobLog.jobcode ).one()

		insertJobLog = JobsLog(
			source_hash = srcHash,
			person_id = srcGetPersonId.id,
			department_id = srcGetDepartmentId.id,
			job_id = srcGetJobId.id,
			emplid = srcJobLog.emplid,
			deptid = srcJobLog.deptid,
			jobcode = srcJobLog.jobcode,
			supervisor_id = srcJobLog.supervisor_id,
			main_appt_num_jpn = srcJobLog.main_appt_num_jpn,
			effdt = srcJobLog.effdt,
			action = srcJobLog.action,
			action_reason = srcJobLog.action_reason,
			action_dt = srcJobLog.action_dt,
			job_entry_dt = srcJobLog.job_entry_dt,
			dept_entry_dt = srcJobLog.dept_entry_dt,
			position_entry_dt = srcJobLog.position_entry_dt,
			hire_dt = srcJobLog.hire_dt,
			last_hire_dt = srcJobLog.last_hire_dt,
			termination_dt = srcJobLog.termination_dt,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertJobLog

def getTargetJobsLog( sesTarget ):
	"""
		Returns a set of JobsLog objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		JobsLog ).filter(
			JobsLog.deleted_at.is_( None ) ).all()

def softDeleteJobLog( tgtRecord, srcRecords ):
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
			srcRecord.deptid == tgtRecord.deptid and
			srcRecord.jobcode == tgtRecord.jobcode and
			srcRecord.effdt == tgtRecord.effdt and
			srcRecord.action == tgtRecord.action and
			srcRecord.action_reason == tgtRecord.action_reason 
			for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')


