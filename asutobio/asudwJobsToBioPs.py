from models.asudwpsmodels import AsuDwPsJobs, AsuPsBioFilters
from models.biopsmodels import BioPsJobs

from sharedProcesses import hashThisList


# the data pull
def getSourceJobsData( sesSource ):
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

# the data load
def processJobsData( personJob ):
	"""
		Process an AsuDwPsJobs object and prepare it for insert into the target BioPsJobs table
		@return: the sa add object 
	"""
	personJobList = [
		personJob.emplid,
		personJob.empl_rcd,
		personJob.title,
		personJob.department,
		personJob.mailcode,
		personJob.empl_class,
		personJob.job_indicator,
		personJob.location,
		personJob.hr_status,
		personJob.deptid,
		personJob.empl_status,
		personJob.fte,
		personJob.last_update,
		personJob.department_directory ]

	personJobHash = hashThisList( personJobList )

	tgtPersonJob = BioPsJobs(
		source_hash = personJobHash,
		emplid = personJob.emplid,
		empl_rcd = personJob.empl_rcd,
		title = personJob.title,
		department = personJob.department,
		mailcode = personJob.mailcode,
		empl_class = personJob.empl_class,
		job_indicator = personJob.job_indicator,
		location = personJob.location,
		hr_status = personJob.hr_status,
		deptid = personJob.deptid,
		empl_status = personJob.empl_status,
		fte = personJob.fte,
		last_update = personJob.last_update,
		department_directory = personJob.department_directory)

	return tgtPersonJob
