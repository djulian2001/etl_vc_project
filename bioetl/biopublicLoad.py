import datetime

from sqlalchemy import *
from sqlalchemy.orm import *

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
# Load the mysql.bio_ps people table in into the final destination:
#	mysql:
#		bio_ps.people to bio_public.people
# 	

import personProcessing

# srcPeople = sesSource.query( BioPsPeople ).all()
srcPeople = personProcessing.getSourcePeople( sesSource )

iPerson = 1
for srcPerson in srcPeople:

	try:
		personStatus = personProcessing.processPerson( srcPerson, sesTarget )
	except TypeError as e:
		# print e
		pass
	else:
		sesTarget.add( personStatus )

		if iPerson % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception, e:
				sesTarget.rollback()
				raise e
		iPerson += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e

# need to update the update_flags if they exist...

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
			except Exception, e:
				sesTarget.rollback()
				raise e
		iMissingPerson += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e



###############################################################################
# Load the mysql.bio_ps people data table in into the final destination:
#	mysql:
#		bio_ps.person_phones to bio_public.person_phones
# 	
# Using Group By on the source to limit likely duplicates.

import personPhoneProcessing

srcPersonPhones = personPhoneProcessing.getSourcePhones( sesSource )

iPersonPhone = 1
for srcPersonPhone in srcPersonPhones:
	
	try:
		processPhone = personPhoneProcessing.processPhone( srcPersonPhone, sesTarget )
	except Exception, e:
		print e
		# raise e
		pass
	else:
		sesTarget.add( processPhone )

		if iPersonPhone % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception, e:
				sesTarget.rollback()
				raise e
		iPersonPhone += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e

# Remove the data that was no longer was found for an active person.
tgtMissingPersonPhones = sesTarget.query(
	Phones ).filter( 
		Phones.updated_flag == False ).join(
	People ).filter(
		People.deleted_at.isnot( None ) ).all()

iRemovePhone = 1
for tgtMissingPersonPhone in tgtMissingPersonPhones:
	# if the phone no longer is found we remove it but only if the person is active...
	( foundPhoneExists, ), = sesSource.query(
			exists().where( 
				BioPsPhones.emplid == tgtMissingPersonPhone.emplid ).where(
				BioPsPhones.phone_type == tgtMissingPersonPhone.phone_type ).where(
				BioPsPhones.phone == tgtMissingPersonPhone.phone) )

	if foundPhoneExists == False:

		sesTarget.delete( tgtMissingPersonPhone )


	if iRemovePhone % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e
	iRemovePhone += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e

# reset the updated_flag for all records for the next round of changes.
# engineTarget.execute("UPDATE person_phones SET updated_flag = 0;")
sesTarget.execute( text( "UPDATE person_phones SET updated_flag = :resetFlag" ),{ "resetFlag" : 0 } )
""""""
try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e


###############################################################################
# Load the mysql.bio_ps people data table in into the final destination:
#	mysql:
#		bio_ps.person_addresses to bio_public.person_addresses
# 	
# Using Group By on the source to limit likely duplicates.
srcPersonAddresses = sesSource.query(
	BioPsAddresses ).group_by(
		BioPsAddresses.emplid ).group_by(
		BioPsAddresses.address_type ).group_by(
		BioPsAddresses.source_hash ).all()

iPersonAddresses = 1
for srcPersonAddress in srcPersonAddresses:
	# determine the person exists in the target database.
	( personAddressExists, ), = sesTarget.query(
		exists().where(
			Addresses.emplid == srcPersonAddress.emplid ).where( 
			Addresses.address_type == srcPersonAddress.address_type )	)
	
	if personAddressExists == True:
		# determine if the person that exists requires an update.
		( noPersonAddressUpdate, ), = sesTarget.query(
			exists().where( 
				Addresses.emplid == srcPersonAddress.emplid ).where( 
				Addresses.address_type == srcPersonAddress.address_type ).where(
				Addresses.source_hash == srcPersonAddress.source_hash ) )

		if noPersonAddressUpdate == False:
			# update the the database with the data changes.
			# because of the nature of the one to many
			#	we need to 'illiminate' the records which have NOT been updated, 
			#	to update the correct record.
			# 
			# we need to get the person_address.id to then select the record that needs to be updated.
			# get a list of the address records
			# tgtAddressList = sesTarget.query(
			updatePersonAddress = sesTarget.query(
				Addresses ).filter( 
					Addresses.emplid == srcPersonAddress.emplid ).filter( 
					Addresses.address_type == srcPersonAddress.address_type ).filter( 
					Addresses.source_hash != srcPersonAddress.source_hash ).filter(
					Addresses.updated_flag == False ).first()

			# for tgtAddress in tgtAddressList:
				# which record exists in the source unchanged?
				# ( srcAddressToUpdate, ), = SrcSession.query(
				# 	exists().where( 
				# 		BioPsAddresses.emplid == tgtAddress.emplid ).where( 
				# 		BioPsAddresses.address_type == tgtAddress.address_type ).where( 
				# 		BioPsAddresses.source_hash != tgtAddress.source_hash ) )
				
				# if srcAddressToUpdate == True:
			# updateAddressId = tgtAddress.id
			# 		# break

			# updatePersonAddress = sesTarget.query(
			# 	Addresses ).filter(
			# 		Addresses.id == updateAddressId ).one()

			# update the source_hash to the new hash.
			updatePersonAddress.source_hash = srcPersonAddress.source_hash
			# updates from people soft
			updatePersonAddress.updated_flag = True
			updatePersonAddress.address1 = srcPersonAddress.address1
			updatePersonAddress.address2 = srcPersonAddress.address2
			updatePersonAddress.address3 = srcPersonAddress.address3
			updatePersonAddress.address4 = srcPersonAddress.address4
			updatePersonAddress.city = srcPersonAddress.city
			updatePersonAddress.state = srcPersonAddress.state
			updatePersonAddress.postal = srcPersonAddress.postal
			updatePersonAddress.country_code = srcPersonAddress.country_code
			updatePersonAddress.country_descr = srcPersonAddress.country_descr
			# update the bio timestamp
			updatePersonAddress.updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
			# add the object to the session to commit the updated
			sesTarget.add( updatePersonAddress )

			print "UPDATED_AT: " + updatePersonAddress.updated_at
			
	else:
		
		tgtPerson = sesTarget.query(
			People ).filter(
				People.emplid == srcPersonAddress.emplid ).one()

		tgtInsertPersonAddress = Addresses(
			person_id = tgtPerson.id,
			source_hash = srcPersonAddress.source_hash,
			updated_flag = True,
			emplid = srcPersonAddress.emplid,
			address_type = srcPersonAddress.address_type,
			address1 = srcPersonAddress.address1,
			address2 = srcPersonAddress.address2,
			address3 = srcPersonAddress.address3,
			address4 = srcPersonAddress.address4,
			city = srcPersonAddress.city,
			state = srcPersonAddress.state,
			postal = srcPersonAddress.postal,
			country_code = srcPersonAddress.country_code,
			country_descr = srcPersonAddress.country_descr,
			created_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
		)

		sesTarget.add( tgtInsertPersonAddress )

	if iPersonAddresses % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e
	iPersonAddresses += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e

# REMOVE non-existent data.....
tgtMissingPersonAddresses = sesTarget.query(
	Addresses ).filter( 
		Addresses.updated_flag == False ).join(
	People ).filter(
		People.deleted_at.isnot( None ) ).all()

iRemoveAddresses = 1
for tgtMissingPersonAddress in tgtMissingPersonAddresses:
	# if the phone no longer is found we remove it but only if the person is active...
	( foundAddress, ), = sesSource.query(
			exists().where( 
				BioPsPhones.emplid == tgtMissingPersonAddress.emplid ).where(
				BioPsPhones.address_type == tgtMissingPersonAddress.address_type ).where(
				BioPsPhones.address1 == tgtMissingPersonAddress.address1 ).where(
				BioPsPhones.address1 == tgtMissingPersonAddress.address2 ).where(
				BioPsPhones.address1 == tgtMissingPersonAddress.address3 ).where(
				BioPsPhones.address1 == tgtMissingPersonAddress.address4 ) )

	if foundAddress == False:

		sesTarget.delete( tgtMissingPersonAddress )


	if iRemoveAddresses % 1000 == 0:
		try:
			sesTarget.flush()
		except Exception, e:
			sesTarget.rollback()
			raise e
	iRemoveAddresses += 1

try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e

# reset the updated_flag for all records for the next round of changes.
sesTarget.execute( text( "UPDATE person_addresses SET updated_flag = :resetFlag" ),{ "resetFlag" : 0 } )
""""""
try:
	sesTarget.commit()
except Exception, e:
	sesTarget.rollback()
	raise e
finally:
	sesTarget.close()
	sesSource.close()
