import datetime

from bioetl.sharedProcesses import hashThisList

from models.biopublicmodels import SubAffiliations
from models.asudwpsmodels import BiodesignSubAffiliations

def getTableName():
	return SubAffiliations.__table__.name

def getSourceData( sesTarget=None, qryList=None ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the SubAffiliations table of the source database
		
		@sesTarget:
			is a break in this applications convention.  Normal is sesSource, but in this case
			the source is the target database, a controlled vocabulary of terms, also a config
			source for where in values.
		@qryList:
			probaly could remove this but there is no reason too.

		Returns:
			list of sqlalchemy dictionary objects.
	"""

	srcSubAffiliations = sesTarget.query( SubAffiliations ).all()

	if srcSubAffiliations:
		return srcSubAffiliations
	else:
		seeds = BiodesignSubAffiliations.seedMe()
		# srcSubAffiliations = [ BiodesignSubAffiliations( **seed ) for seed in seeds ]
		srcSubAffiliations = [ BiodesignSubAffiliations( **seed ) for seed in seeds ]

        return srcSubAffiliations


# change value to the singular
def processData( srcSubAffiliation, sesTarget ):
	"""
		Takes in a source SubAffiliation object from biopsmodels (mysql.bio_ps.SubAffiliations)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.SubAffiliations), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""


################################################
# I'm running into an issue when in dict land and sqlalchemy object land
# LOOK at casting all of the dict objects into BiodesignSubAffiliations for db
# initial seeding in the getSourceData()
#	we have to get srcRecord["column"] into srcRecord.column
#
#

#
	if isinstance( srcSubAffiliation, SubAffiliations ):
		subAffiliationList = [
			srcSubAffiliation.code,
			srcSubAffiliation.title,
			srcSubAffiliation.description,
			srcSubAffiliation.proximity_scope,
			srcSubAffiliation.service_access,
			srcSubAffiliation.distribution_lists ]

		srcHash = hashThisList( subAffiliationList )
		srcSubAffiliationCode = srcSubAffiliation.code
	else:
		srcHash = hashThisList( srcSubAffiliation.values() )
		srcSubAffiliationCode = srcSubAffiliation["code"]




	def getTargetRecords():
		"""Returns a record set from the target database"""
		ret = sesTarget.query(
			SubAffiliations ).filter(
				SubAffiliations.code == srcSubAffiliationCode ).filter(
				SubAffiliations.updated_flag == False ).all()

		return ret

# fix code above and below to match sqlalchemy orm class object...
################################################

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

			tgtRecord.updated_flag = True
			tgtRecord.source_hash = srcHash
			# list of the fields that will be updated...
			tgtRecord.code = srcSubAffiliation["code"]
			tgtRecord.title = srcSubAffiliation["title"]
			tgtRecord.description = srcSubAffiliation["description"]
			tgtRecord.proximity_scope = srcSubAffiliation["proximity_scope"]
			tgtRecord.service_access = srcSubAffiliation["service_access"]
			tgtRecord.distribution_lists = srcSubAffiliation["distribution_lists"]
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None

			return tgtRecord
	else:
		insertSubAffiliation = SubAffiliations(
			updated_flag = True,
			source_hash = srcHash,
			code = srcSubAffiliation["code"],
			title = srcSubAffiliation["title"],
			description = srcSubAffiliation["description"],
			proximity_scope = srcSubAffiliation["proximity_scope"],
			service_access = srcSubAffiliation["service_access"],
			distribution_lists = srcSubAffiliation["distribution_lists"],
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertSubAffiliation

def getTargetData( sesTarget ):
	"""
		Returns a set of SubAffiliations objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		SubAffiliations ).filter(
			SubAffiliations.deleted_at.is_( None ) ).all()

def softDeleteData( tgtMissingSubAffiliation, srcList ):
	"""
		The list of SubAffiliations changes as time moves on, the SubAffiliations removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a subAffiliation object.
	"""

	def dataMissing():
		"""
			The origional list of selected data is then used to see if data requires a soft-delete
			@True: Means update the records deleted_at column
			@False: Do nothing
		"""
		return not any( srcDict["code"] == tgtMissingSubAffiliation.code for srcDict in srcList )

	if dataMissing():
		tgtMissingSubAffiliation.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtMissingSubAffiliation

