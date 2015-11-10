from models.asudwpsmodels import AsuDwPsJobLog, AsuPsBioFilters
from models.biopsmodels import BioPs_Y_

from sharedProcesses import hashThisList


# the data pull
def getSourceJobLogData( sesSource ):
	"""
		Selects the data from the data wharehouse for the JobLog model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

    subJobLog = (
        self.sesSource.query(
            AsuDwPsJobLog.emplid, AsuDwPsJobLog.jobcode, AsuDwPsJobLog.deptid, AsuDwPsJobLog.effdt, AsuDwPsJobLog.action, 
            func.row_number().over(
                partition_by=[AsuDwPsJobLog.emplid, AsuDwPsJobLog.main_appt_num_jpn],
                order_by=AsuDwPsJobLog.effdt.desc() ).label( 'rn' ) ).join(	
    		srcEmplidsSubQry, AsuDwPsJobLog.emplid == srcEmplidsSubQry.c.emplid ) ).subquery()

	return sesSource.query(
		AsuDwPsJobLog ).join(
			
			)

# the data load
def process_Y_Data():
	"""
		Process an AsuDwPsJobLog object and prepare it for insert into the target BioPs_Y_ table
		@return: the sa add object 
	"""
	pass
