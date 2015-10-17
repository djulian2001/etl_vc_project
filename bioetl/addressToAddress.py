import datetime
from sqlalchemy import *
from sqlalchemy.orm import *

from peopleSoftTables import *
from bioPublicTables import *
# connections...
# eng_src = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public_ps', echo=True)
eng_src = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public_ps')
# eng_tgt = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public', echo=True)
eng_tgt = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public')

# BioPublic.metadata.drop_all(eng_tgt)

BioPublic.metadata.create_all(eng_tgt)

# source_conn = eng_src.connect()
# bind the model to eng_tgt engine
PsPublic.metadata.bind = eng_src
BioPublic.metadata.bind = eng_tgt

SrcSession = scoped_session(sessionmaker(bind=eng_src))
TgtSession = scoped_session(sessionmaker(bind=eng_tgt))

true, false = literal(True), literal(False)

# sesSource_people = SrcSession()
# sesTarget_people = TgtSession()

sesSource = SrcSession()
sesTarget = TgtSession()

# Using Group By on the source to limit likely duplicates.
srcPersonAddresses = sesSource.query(
	PsAddresses ).group_by(
		PsAddresses.emplid ).group_by(
		PsAddresses.address_type ).group_by(
		PsAddresses.source_hash ).all()

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
				# 		PsAddresses.emplid == tgtAddress.emplid ).where( 
				# 		PsAddresses.address_type == tgtAddress.address_type ).where( 
				# 		PsAddresses.source_hash != tgtAddress.source_hash ) )
				
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

		tgtInsertPerson = Addresses(
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

		sesTarget.add( tgtInsertPerson )

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
finally:
	sesTarget.close() 
