from models.asudwpsmodels import AsuDwPs_X_, AsuPsBioFilters
from models.biopsmodels import BioPs_X_

from sharedProcesses import hashThisList

# _X_  : first char caps plural,
# _Y_ caps singular,  
# _y_ lower case singular
# _Z_ all caps w/ underscore, -oracle table name
# _ZZ_ all caps w/ underscore, -oracle schema name
# _z_ lower case w/ undersores - mysql table name


# the data pull
def getSource_X_Data( sesSource ):
	"""
		Selects the data from the data wharehouse for the _X_ model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPs_X_ ).join(
		)

# the data load
def process_Y_Data( _y_ ):
	"""
		Process an AsuDwPs_X_ object and prepare it for insert into the target BioPs_X_ table
		@return: the sa add object 
	"""
	
	_y_List = []

	_y_Hash = hashThisList( _y_List )

	tgt_Y_ = BioPs_X_(
		source_hash = _y_Hash,
		
		_attribute_list_here_
		
		)

	return tgt_Y_



###############################################################################
# Cut and paste below
###############################################################################


###############################################################################
# Extract the oracle table and load it into a mysql table:
# 	oracle:
#		_ZZ_._Z_
#	mysql:
#		_z_
#

	import asudw_Y_ToBioPs
	
	src_Y_ = asudw_Y_ToBioPs.getSource_X_Data( sesSource )

	i_Y_ = 1
	for _y_ in src_Y_:
		
		add_Y_ = asudw_Y_ToBioPs.process_Y_Data( _y_ )
		
		sesTarget.add( add_Y_ )

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

#
# end of for src_Y_
###############################################################################

###############################################################################
# Cut and add to the models 
###############################################################################

class AsuDwPs_X_( AsuDwPs ):
	__tablename__ = '_Z_'
	schema = '_ZZ_'

	_attribute_list_here_

	__mapper_args__ = { "primary_key" : [ ] }
	__table_args__ = { "schema" : schema }


class BioPs_X_( BioPs ):
	__tablename__ = '_z_'
	
	_attribute_list_here_


class _X_( BioPublic ):
	__tablename__ = '_z_'

	person_id = Column( Integer, ForeignKey( 'people.id' ) )
    # below are the data fields out of peopleSoft
    _attribute_list_here_



###############################################################################
# Cut and add to the bioetl files..... 
###############################################################################


import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import _X_
from asutobio.models.biopsmodels import BioPs_X_


def getSource_X_( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the _X_ table of the source database.
	"""

	return sesSource.query( BioPs_X_ ).all()

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

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

	def _y_Exists():
		"""
			determine the _y_ exists in the target database.
			@True: The _y_ exists in the database
			@False: The _y_ does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				_X_._yyy_ == src_Y_._yyy_ ) )

		return ret

	if _y_Exists():

		def _y_UpdateRequired():
			"""
				Determine if the _y_ that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					_X_._yyy_ == src_Y_._yyy_ ).where(
					_X_.source_hash == src_Y_.source_hash ) )

			return not ret

		if _y_UpdateRequired():
			# retrive the tables object to update.
			update_Y_ = sesTarget.query(
				_X_ ).filter(
					_X_._yyy_ == src_Y_._yyy_ ).one()

			# repeat the following pattern for all mapped attributes:
			update_Y_.source_hash = src_Y_.source_hash
			update_Y_._yyy_ = src_Y_._yyy_

			update_Y_.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			update_Y_.deleted_at = None

			return update_Y_
		else:
			raise TypeError('source _y_ already exists and requires no updates!')

	else:
		insert_Y_ = _X_(
			source_hash = src_Y_.source_hash,
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

def softDelete_Y_( tgtMissing_Y_, sesSource ):
	"""
		The list of _X_ changes as time moves on, the _X_ removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flag_Y_Missing():
		"""
			Determine that the _y_ object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPs_X_._yyy_ == tgtMissing_Y_._yyy_ ) )

		return not ret

	if flag_Y_Missing():

		tgtMissing_Y_.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissing_Y_

	else:
		raise TypeError('source person still exists and requires no soft delete!')


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
		remove_Y_ = _y_Processing.softDelete_Y_( tgtMissing_Y_, sesSource )
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



