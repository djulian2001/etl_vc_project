import datetime
from sqlalchemy.orm.exc import NoResultFound

from bioetl.sharedProcesses import hashThisList, AsuPsBioFilters
from models.biopublicmodels import JobsLog, People, Departments, JobCodes
from models.asudwpsmodels import AsuDwPsJobsLog

def getTableName():
	return JobsLog.__table__.name

def getSourceData( sesSource, appState=None, qryList=None ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the JobsLog table of the source database.

		filtering the data on HR action codes:
			'HIR', 'REH', 'RET', 'TER', 'XFR'
	"""
	if not qryList:	
		srcFilters = AsuPsBioFilters( sesSource, appState.subAffCodes )

		srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

		return sesSource.query(
			AsuDwPsJobsLog ).join(
				srcEmplidsSubQry, AsuDwPsJobsLog.emplid==srcEmplidsSubQry.c.emplid ).filter(
				AsuDwPsJobsLog.action.in_( ('HIR','REH','RET','TER','XFR') ) ).all()
	else:
		return sesSource.query(
			AsuDwPsJobsLog ).filter(
				AsuDwPsJobsLog.emplid.in_( qryList ) ).filter(
				AsuDwPsJobsLog.action.in_( ('HIR','REH','RET','TER','XFR') ) ).all()

def processData( srcJobLog, sesTarget ):
	"""
		Takes in a source JobLog object from biopsmodels (mysql.bio_ps.JobsLog)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.JobsLog), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	jobLogList = [
		srcJobLog.emplid,
		srcJobLog.deptid,
		srcJobLog.jobcode,
		srcJobLog.supervisor_id,
		srcJobLog.reports_to,
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

	def getTargetRecords():
		"""
			determine the jobLog exists in the target database.
			@return recordset of jobLog objects
		"""
		ret = sesTarget.query(
			JobsLog ).filter(
				JobsLog.emplid == srcJobLog.emplid ).filter(
				JobsLog.deptid == srcJobLog.deptid ).filter(
				JobsLog.jobcode == srcJobLog.jobcode ).filter(
				JobsLog.effdt == srcJobLog.effdt ).filter(
				JobsLog.action == srcJobLog.action ).filter(
				JobsLog.action_reason == srcJobLog.action_reason ).filter(
				JobsLog.updated_flag == False ).all()

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
			tgtRecord.emplid = srcJobLog.emplid
			tgtRecord.deptid = srcJobLog.deptid
			tgtRecord.jobcode = srcJobLog.jobcode
			tgtRecord.supervisor_id = srcJobLog.supervisor_id
			tgtRecord.reports_to = srcJobLog.reports_to
			tgtRecord.main_appt_num_jpn = srcJobLog.main_appt_num_jpn
			tgtRecord.effdt = srcJobLog.effdt
			tgtRecord.action = srcJobLog.action
			tgtRecord.action_reason = srcJobLog.action_reason
			tgtRecord.action_dt = srcJobLog.action_dt
			tgtRecord.job_entry_dt = srcJobLog.job_entry_dt
			tgtRecord.dept_entry_dt = srcJobLog.dept_entry_dt
			tgtRecord.position_entry_dt = srcJobLog.position_entry_dt
			tgtRecord.hire_dt = srcJobLog.hire_dt
			tgtRecord.last_hire_dt = srcJobLog.last_hire_dt
			tgtRecord.termination_dt = srcJobLog.termination_dt
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None

			return tgtRecord

	else:
		try:
			srcGetPersonId = sesTarget.query(
				People.id ).filter(
					People.emplid == srcJobLog.emplid ).one()

		except NoResultFound as e:
			raise e

		else:
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
				reports_to = srcJobLog.reports_to,
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

def getTargetData( sesTarget ):
	"""
		Returns a set of JobsLog objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		JobsLog ).filter(
			JobsLog.deleted_at.is_( None ) ).all()

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
			srcRecord.deptid == tgtRecord.deptid and
			srcRecord.jobcode == tgtRecord.jobcode and
			srcRecord.effdt == tgtRecord.effdt and
			srcRecord.action == tgtRecord.action and
			srcRecord.action_reason == tgtRecord.action_reason 
			for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
