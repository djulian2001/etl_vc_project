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

# src_session_people = SrcSession()
# tgt_session_people = TgtSession()

src_session = SrcSession()
tgt_session = TgtSession()

# Using Group By on the source to limit likely duplicates.
src_addresses = src_session.query(PsAddresses).group_by(PsAddresses.emplid).group_by(PsAddresses.address_type).group_by(PsAddresses.source_hash).all()

i = 1
for src_address in src_addresses:
	# determine the person exists in the target database.
	(address_exists, ), = TgtSession.\
		query(\
			exists().\
				where( Addresses.emplid==src_address.emplid ).\
				where( Addresses.address_type==src_address.address_type )\
		)
	if address_exists == True:
		# determine if the person that exists requires an update.
		(no_update_required, ), = TgtSession.\
			query(\
				exists().\
					where( Addresses.emplid==src_address.emplid ).\
					where( Addresses.address_type==src_address.address_type ).\
					where( Addresses.source_hash==src_address.source_hash )\
			)

		if no_update_required == True:
			# print i
			pass
		else:
			# update the the database with the data changes.
			# because of the nature of the one to many
			#	we need to 'illiminate' the records which have NOT been updated, 
			#	to update the correct record.
			# 
			# we need to get the person_address.id to then select the record that needs to be updated.
			# get a list of the address records
			tgt_address_list = tgt_session.\
				query(Addresses).\
					filter( Addresses.emplid==src_address.emplid ).\
					filter( Addresses.address_type==src_address.address_type ).\
					filter( Addresses.source_hash!=src_address.source_hash).\
				all()

			for tgt_address in tgt_address_list:
				# which record exists in the source unchanged?
				(src_updated, ), = SrcSession.\
					query(\
						exists().\
							where( PsAddresses.emplid==tgt_address.emplid ).\
							where( PsAddresses.address_type==tgt_address.address_type ).\
							where( PsAddresses.source_hash!=tgt_address.source_hash )\
					)
				if src_updated == True:
					update_this_id = tgt_address.id
					break

			update_person_address = tgt_session.\
				query(Addresses).\
					filter( Addresses.id == update_this_id ).\
				one()

			# update the source_hash to the new hash.
			update_person_address.source_hash = src_address.source_hash
			# updates from people soft
			update_person_address.address1 = src_address.address1
			update_person_address.address2 = src_address.address2
			update_person_address.address3 = src_address.address3
			update_person_address.address4 = src_address.address4
			update_person_address.city = src_address.city
			update_person_address.state = src_address.state
			update_person_address.postal = src_address.postal
			update_person_address.country_code = src_address.country_code
			update_person_address.country_descr = src_address.country_descr
			# update the bio timestamp
			update_person_address.updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
			# add the object to the session to commit the updated
			tgt_session.add(update_person_address)

			print "UPDATED_AT: " + update_person_address.updated_at
			
	else:
		# person wasn't in the target databases, add them now
		# get the person_id from the bio_public.people table...
		# note: might have to test the existance of the person prior... 
		# 	but lets assume our origional scrits worked and there is only one emplid (for now)
		src_person = tgt_session.\
			query(People).\
				filter(People.emplid==src_address.emplid).\
			one()

		tgt_person = Addresses(
			person_id = src_person.id,
			source_hash = src_address.source_hash,
			emplid = src_address.emplid,
			address_type = src_address.address_type,
			address1 = src_address.address1,
			address2 = src_address.address2,
			address3 = src_address.address3,
			address4 = src_address.address4,
			city = src_address.city,
			state = src_address.state,
			postal = src_address.postal,
			country_code = src_address.country_code,
			country_descr = src_address.country_descr,
			created_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
		)

		tgt_session.add(tgt_person)

	if i % 1000 == 0:
		try:
			tgt_session.flush()
		except Exception, e:
			tgt_session.rollback()
			raise e
	i += 1

try:
	tgt_session.commit()
except Exception, e:
	tgt_session.rollback()
	raise e
finally:
	tgt_session.close() 
