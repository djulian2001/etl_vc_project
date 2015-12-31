###############################################################################
# The following is a template file that can be used to quickly add new data tables to the data warehouse data pulls.
# Parts of the script will require changes for the specific conditions that will differ, note the attribute _yyy_,
# as well as the data pull from the DW, the models might require other tables, particular tables that have reference
# dependency.
#
# List of <control><command><g> replacements:
# 
# _X_  : first char caps plural,
# _Y_ caps singular,  
# _y_ lower case singular
# _Z_ all caps w/ underscore, -oracle table name
# _ZZ_ all caps w/ underscore, -oracle schema name
# _z_ lower case w/ undersores - mysql table name
#
#
# bioetl\ filename		_y_Processing.py

###############################################################################
# Cut and add to the models 
###############################################################################

class AsuDwPs_X_( AsuDwPs ):
	__tablename__ = '_Z_'
	schema = '_ZZ_'

	_attribute_list_here_

	__mapper_args__ = { "primary_key" : [ ] }
	__table_args__ = { "schema" : schema }


class _X_( BioPublic ):
	__tablename__ = '_z_'

	person_id = Column( Integer, ForeignKey( 'people.id' ) )
    # below are the data fields out of peopleSoft
    _attribute_list_here_



###############################################################################
# Cut and add to the bioetl files..... 
###############################################################################


import datetime

from sharedProcesses import hashThisList
from models.biopublicmodels import _X_
from models.asudwpsmodels import AsuDwPs_X_, AsuPsBioFilters

def getSource_X_( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the _X_ table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPs_X_ ).join(
			).order_by(
				AsuDwPs___ ).all()

# change value to the singular
def process_Y_( src_Y_, sesTarget ):
	"""
		Takes in a source _Y_ object from biopsmodels (mysql.bio_ps._X_)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public._X_), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	_y_List = []

	srcHash = hashThisList( _y_List )

	def getTargetRecords():
		"""Returns a record set from the target database."""
		ret = sesTarget.query(
			_X_ ).filter(
				_X_._yyy_ == src_Y_._yyy_ ) )

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

			tgtRecord.updated_flag = True
			tgtRecord.source_hash = srcHash
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None
			# list of the fields that will be updated...

			# ^ 
			return tgtRecord
			
	else:
		insert_Y_ = _X_(
			source_hash = srcHash,
			updated_flag = True,
			_yyy_ = src_Y_._yyy_,
			...
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insert_Y_

def getTarget_X_( sesTarget ):
	"""
		Returns a set of _X_ objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		_X_ ).filter(
			_X_.deleted_at.is_( None ) ).all()

def softDelete_Y_( tgtRecord, srcRecords ):
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
		return not any( srcRecord._yyy_ == tgtRecord._yyy_ for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')

###############################################################################
### biopublicLoad.py cut and paste into proper stop in the file all below:
###############################################################################

	###############################################################################
	# 
	#   File Import:  _y_Processing

	import _y_Processing

	src_X_ = _y_Processing.getSource_X_( sesSource )

	i_Y_ = 1
	for src_Y_ in src_X_:
		try:
			processed_y_ = _y_Processing.process_Y_( src_Y_, sesTarget )
		except TypeError as e:
			pass
		else:
			sesTarget.add( processed_y_ )
			if i_Y_ % 1000 == 0:
				try:
					sesTarget.flush()
				except Exception as e:
					sesTarget.rollback()
					raise e
			i_Y_ += 1
	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e


	tgtMissing_X_ = _y_Processing.getTarget_X_( sesTarget )

	iRemove_Y_ = 1
	for tgtMissing_Y_ in tgtMissing_X_:
		try:
			remove_Y_ = _y_Processing.softDelete_Y_( tgtMissing_Y_, src_X_ )
		except TypeError as e:
			pass
		else:
			sesTarget.add( remove_Y_ )
			if iRemove_Y_ % 1000 == 0:
				try:
					sesTarget.flush()
				except Exception as e:
					sesTarget.rollback()
					raise e
			iRemove_Y_ += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e

	#	End of _y_Processing
	###############################################################################



