import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import Departments
from asutobio.models.biopsmodels import BioPsDepartments

#template mapping... plural Departments    singularCaped Department   singularLower department 

def getSourceDepartments( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the Departments table of the source database.
	"""

	return sesSource.query( BioPsDepartments ).all()

# change value to the singular
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

#template mapping... column where(s) deptid 

	def departmentExists( deptid ):
		"""determine the department exists in the target database."""
		(ret, ), = sesTarget.query(
			exists().where(
				Departments.deptid == deptid ) )

		return ret

	if departmentExists( srcDepartment.deptid ):

		def departmentUpdateRequired( deptid, source_hash ):
			"""
				Determine if the department that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					Departments.deptid == deptid).where(
					Departments.source_hash == source_hash ) )
			return not ret

		if departmentUpdateRequired( srcDepartment.deptid, srcDepartment.source_hash ):

			updateDepartment = sesTarget.query(
				Departments ).filter(
					Departments.deptid == srcDepartment.deptid ).one()

			updateDepartment.source_hash = srcDepartment.source_hash
			updateDepartment.deptid = srcDepartment.deptid
			updateDepartment.effdt = srcDepartment.effdt
			updateDepartment.eff_status = srcDepartment.eff_status
			updateDepartment.descr = srcDepartment.descr
			updateDepartment.descrshort = srcDepartment.descrshort
			updateDepartment.location = srcDepartment.location
			updateDepartment.budget_deptid = srcDepartment.budget_deptid
			updateDepartment.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

			return updateDepartment
		else:
			raise TypeError('source department already exists and requires no updates!')

	else:
		insertDepartment = Departments(
			source_hash = srcDepartment.source_hash,
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

def softDeleteDepartment( tgtMissingDepartment, sesSource ):
	"""
		The list of Departments changes as time moves on, the Departments removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagDepartmentMissing( deptid ):
		"""
			Determine that the department object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsDepartments.deptid == deptid ) )
		
		return not ret

	if flagDepartmentMissing( tgtMissingDepartment.deptid ):

		tgtMissingDepartment.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingDepartment

	else:
		raise TypeError('source person still exists and requires no soft delete!')
