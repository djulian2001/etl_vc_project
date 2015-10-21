import datetime

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.exc import *


from models.biopublicmodels import *
from asutobio.models.biopsmodels import *
# import processController as pc

# from ..asutobio.models.biopsmodels import *

# connections...
# engineSource = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public_ps', echo=True)

sourceDbUser = 'app_asutobioetl'
sourceDbPw = 'forthegipperNotReagan4show'
sourceDbHost = 'dbdev.biodesign.asu.edu'
sourceDbName = 'bio_ps'
engSourceString = 'mysql+mysqldb://%s:%s@%s/%s' % ( sourceDbUser, sourceDbPw, sourceDbHost, sourceDbName )
engineSource = create_engine( engSourceString, echo=True )
# engineSource = create_engine( engSourceString )

targetDbUser = 'app_asutobioetl'
targetDbPw = 'forthegipperNotReagan4show'
targetDbHost = 'dbdev.biodesign.asu.edu'
targetDbName = 'bio_public'
engTargetString = 'mysql+mysqldb://%s:%s@%s/%s' % ( targetDbUser, targetDbPw, targetDbHost, targetDbName )
engineTarget = create_engine( engTargetString, echo=True )
# engineTarget = create_engine( engTargetString )

# Not sure how I'm going to deal with this...
# BioPublic.metadata.drop_all( engineTarget )

BioPublic.metadata.create_all( engineTarget )


# source_conn = engineSource.connect()
# bind the model to engineTarget engine
BioPs.metadata.bind = engineSource
BioPublic.metadata.bind = engineTarget

SrcSession = scoped_session( sessionmaker( bind=engineSource ) )
TgtSession = scoped_session( sessionmaker( bind=engineTarget ) )

true, false = literal(True), literal(False)

# sesSource_people = SrcSession()
# sesTarget_people = TgtSession()

sesSource = SrcSession()
sesTarget = TgtSession()

###############################################################################
# Utility functions...
def resetSourceUpdatedFlag( tblName ):
	"""
		Each run of the script requires that the updated_flag field be set to False
		Build and pass back the string that will do this update.
	"""
	# updateSql = text( "UPDATE %s SET updated_flag = :resetFlag" % ( tblName ) ), { "resetFlag" : 0 }
	updateSql = "UPDATE %s SET updated_flag = :resetFlag" % ( tblName )
	
	return updateSql

# End of Utility functions...
###############################################################################



###############################################################################
# Load the mysql.bio_ps people table in into the final destination:
#	mysql:
#		bio_ps.people to bio_public.people
# 
import personProcessing

srcPeople = personProcessing.getSourcePeople( sesSource )

iPerson = 1
for srcPerson in srcPeople:
	try:
		personStatus = personProcessing.processPerson( srcPerson, sesTarget )
	except TypeError as e:
		pass
	else:
		sesTarget.add( personStatus )
		if iPerson % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
			except RuntimeError as e:
				pass
		iPerson += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# 
# "REMOVE" with a soft delete of person records no longer found in the source database
tgtMissingPeople = personProcessing.getTargetPeople( sesTarget )

iMissingPerson = 1
for tgtMissingPerson in tgtMissingPeople:
	try:
		personMissing = personProcessing.softDeletePerson( tgtMissingPerson, sesSource )
	except TypeError as e:
		# print e
		pass
	else:
		sesTarget.add( personMissing )
		if iMissingPerson % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iMissingPerson += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e
# End of the process for the person data 
###############################################################################


###############################################################################
# Load the mysql.bio_ps people data table in into the final destination:
#	mysql:
#		bio_ps.person_phones to bio_public.person_phones
# 	
# Using Group By on the source to limit likely duplicates.
#
import personPhoneProcessing

srcPersonPhones = personPhoneProcessing.getSourcePhones( sesSource )

iPersonPhone = 1
for srcPersonPhone in srcPersonPhones:
	try:
		processPhone = personPhoneProcessing.processPhone( srcPersonPhone, sesTarget )
	except Exception as e:
		# print e
		# raise e
		pass
	else:
		sesTarget.add( processPhone )
		if iPersonPhone % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonPhone += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# Remove the data that was no longer was found for an active person.
tgtMissingPersonPhones = personPhoneProcessing.getTargetPhones( sesTarget )

iRemovePhone = 1
for tgtMissingPersonPhone in tgtMissingPersonPhones:
	# if the phone no longer is found we remove it but only if the person is active...
	try:
		removePhone = personPhoneProcessing.cleanupSourcePhones()
	except TypeError, e:
		# print e
		pass
	else:
		sesTarget.delete( tgtMissingPersonPhone )
		if iRemovePhone % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemovePhone += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# reset the updated_flag for all records for the next round of changes.
# engineTarget.execute("UPDATE person_phones SET updated_flag = 0;")
try:
	resetPhoneFlags = resetSourceUpdatedFlag( "person_phones" )
except Exception as e:
	print e
else:
	sesTarget.execute( text( resetPhoneFlags ), { "resetFlag" : 0 } )

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e


###############################################################################
# Load the mysql.bio_ps people data table in into the final destination:
#	mysql:
#		bio_ps.person_addresses to bio_public.person_addresses
# 	
# Using Group By on the source to limit likely duplicates.
import personAddressProcessing

srcPersonAddresses = personAddressProcessing.getSourceAddresses( sesSource )

iPersonAddresses = 1
for srcPersonAddress in srcPersonAddresses:
	try:
		processedAddress = personAddressProcessing.processAddress( srcPersonAddress, sesTarget )
	except TypeError as e:
		pass
	else:
		sesTarget.add( processedAddress )
		if iPersonAddresses % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iPersonAddresses += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# REMOVE the data no longer found in the source database...
tgtMissingPersonAddresses = personAddressProcessing.getTargetAddresses( sesTarget )

iRemoveAddresses = 1
for tgtMissingPersonAddress in tgtMissingPersonAddresses:
	try:
		removePersonAddress = personAddressProcessing.cleanupSourceAddresses( tgtMissingPersonAddress )
	except TypeError as e:
		pass
	else:
		sesTarget.delete( tgtMissingPersonAddress )
		if iRemoveAddresses % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e
		iRemoveAddresses += 1
try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e

# RESET the updated_flag for all records for the next round of changes.
try:
	resetAddressFlags = resetSourceUpdatedFlag( "person_addresses" )
except Exception as e:
	print e
else:
	sesTarget.execute( text( resetAddressFlags ), { "resetFlag" : 0 } )

try:
	sesTarget.commit()
except Exception as e:
	sesTarget.rollback()
	raise e
finally:
	sesTarget.close()
	sesSource.close()

# End the processing of person addresses records
###############################################################################
