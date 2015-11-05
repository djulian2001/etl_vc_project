from models.asudwpsmodels import AsuDwPsDepartments, AsuPsBioFilters
from models.biopsmodels import BioPsDepartments

from sharedProcesses import hashThisList
from sqlalchemy import func

# the data pull
def getSourceDepartmentsData( sesSource ):
	"""
		Selects the data from the data wharehouse for the Departments model.  Because of the way
		ASU stores it's departments changes, we have to run a windows (over) function to pull
		the singular department id.  They do not provide the table's primary key.
		@returns: the record set
	"""
	
	subDepartments = (
        sesSource.query(
        	AsuDwPsDepartments,
            func.row_number().over(
                partition_by=[AsuDwPsDepartments.deptid],
                order_by=AsuDwPsDepartments.effdt.desc()
                ).label( 'rn' ) ) ).subquery()

	return sesSource.query(
		subDepartments ).filter(
			subDepartments.c.rn == 1 ).filter(
			subDepartments.c.descr != 'Inactive' ).order_by(
				subDepartments.c.deptid ).all()

# the data load
def processDepartmentsData( srcDepartment ):
	"""
		Process an AsuDwPsDepartments object and prepare it for insert into the target BioPsDepartments table
		@return: the sa add object 
	"""
	departmentList = [
		srcDepartment.deptid,
		srcDepartment.effdt,
		srcDepartment.eff_status,
		srcDepartment.descr,
		srcDepartment.descrshort,
		srcDepartment.location,
		srcDepartment.budget_deptid	]

	departmentHash = hashThisList( departmentList )

	tgtDepartment = BioPsDepartments(
		source_hash = departmentHash,
		deptid = srcDepartment.deptid,
		effdt = srcDepartment.effdt,
		eff_status = srcDepartment.eff_status,
		descr = srcDepartment.descr,
		descrshort = srcDepartment.descrshort,
		location = srcDepartment.location,
		budget_deptid = srcDepartment.budget_deptid	)

	return tgtDepartment