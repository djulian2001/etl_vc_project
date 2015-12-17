import datetime
from sqlalchemy import exists, literal, func

from sharedProcesses import hashThisList
from models.biopublicmodels import Departments
from models.asudwpsmodels import AsuDwPsDepartments

#template mapping... plural Departments    singularCaped Department   singularLower department 

def getSourceDepartments( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the Departments table of the source database.
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


def processDepartment( srcDepartment, sesTarget ):
	"""
		Takes in a source Department object from biopsmodels (mysql.bio_ps.Departments)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.Departments), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	recordToList = [
		srcDepartment.deptid,
		srcDepartment.effdt,
		srcDepartment.eff_status,
		srcDepartment.descr,
		srcDepartment.descrshort,
		srcDepartment.location,
		srcDepartment.budget_deptid	]

	srcHash = hashThisList( recordToList )

	def departmentExists():
		"""determine the department exists in the target database."""
		(ret, ), = sesTarget.query(
			exists().where(
				Departments.deptid == srcDepartment.deptid ) )

		return ret

	if departmentExists():
		def departmentUpdateRequired():
			"""
				Determine if the department that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					Departments.deptid == srcDepartment.deptid).where(
					Departments.source_hash == srcHash ).where(
					Departments.deleted_at.is_( None ) ) )

			return not ret

		if departmentUpdateRequired():

			updateDepartment = sesTarget.query(
				Departments ).filter(
					Departments.deptid == srcDepartment.deptid ).one()

			updateDepartment.source_hash = srcHash
			updateDepartment.deptid = srcDepartment.deptid
			updateDepartment.effdt = srcDepartment.effdt
			updateDepartment.eff_status = srcDepartment.eff_status
			updateDepartment.descr = srcDepartment.descr
			updateDepartment.descrshort = srcDepartment.descrshort
			updateDepartment.location = srcDepartment.location
			updateDepartment.budget_deptid = srcDepartment.budget_deptid
			updateDepartment.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateDepartment.deleted_at = None
			
			return updateDepartment
		else:
			raise TypeError('source department already exists and requires no updates!')
	else:
		insertDepartment = Departments(
			source_hash = srcHash,
			deptid = srcDepartment.deptid,
			effdt = srcDepartment.effdt,
			eff_status = srcDepartment.eff_status,
			descr = srcDepartment.descr,
			descrshort = srcDepartment.descrshort,
			location = srcDepartment.location,
			budget_deptid = srcDepartment.budget_deptid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertDepartment


def getTargetDepartments( sesTarget ):
	"""
		Returns a set of Departments objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		Departments ).filter(
			Departments.deleted_at.is_( None ) ).all()


def softDeleteDepartment( tgtRecord, srcRecords ):
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
		return not any( srcRecord.deptid == tgtRecord.deptid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')

