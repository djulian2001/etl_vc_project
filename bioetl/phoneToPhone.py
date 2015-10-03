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
src_phones = src_session.\
					query(PsPhones).\
						group_by(PsPhones.emplid).\
						group_by(PsPhones.phone_type).\
						group_by(PsPhones.source_hash).\
					all()

i = 1
for src_phone in src_phones:
	# determine the person exists in the target database.
	(phone_exists, ), = TgtSession.\
		query(\
			exists().\
				where( Phones.emplid==src_phone.emplid ).\
				where( Phones.phone_type==src_phone.phone_type )\
		)
	if phone_exists == True:
		# determine if the person that exists requires an update.
		(no_update_required, ), = TgtSession.\
			query(\
				exists().\
					where( Phones.emplid==src_phone.emplid ).\
					where( Phones.phone_type==src_phone.phone_type ).\
					where( Phones.source_hash==src_phone.source_hash )\
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
			tgt_phone_list = tgt_session.\
				query(Phones).\
					filter( Phones.emplid==src_phone.emplid ).\
					filter( Phones.phone_type==src_phone.phone_type ).\
					filter( Phones.source_hash!=src_phone.source_hash).\
				all()

			for tgt_address in tgt_phone_list:
				# which record exists in the source unchanged?
				(src_updated, ), = SrcSession.\
					query(\
						exists().\
							where( PsPhones.emplid==tgt_address.emplid ).\
							where( PsPhones.phone_type==tgt_address.phone_type ).\
							where( PsPhones.source_hash!=tgt_address.source_hash )\
					)
				if src_updated == True:
					update_this_id = tgt_address.id
					break

			update_person_phone = tgt_session.\
				query(Phones).\
					filter( Phones.id == update_this_id ).\
				one()

			# update the source_hash to the new hash.
			update_person_phone.source_hash = src_phone.source_hash
			# updates from people soft
			update_person_phone.phone = src_phone.phone
			# update the bio timestamp
			update_person_phone.updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
			# add the object to the session to commit the updated
			tgt_session.add(update_person_phone)

			print "UPDATED_AT: " + update_person_phone.updated_at
			
	else:
		# person wasn't in the target databases, add them now
		# get the person_id from the bio_public.people table...
		# note: might have to test the existance of the person prior... 
		# 	but lets assume our origional scrits worked and there is only one emplid (for now)
		src_person = tgt_session.\
			query(People).\
				filter(People.emplid==src_phone.emplid).\
			one()

		tgt_person = Phones(
			person_id = src_person.id,
			source_hash = src_phone.source_hash,
			emplid = src_phone.emplid,
			phone_type = src_phone.phone_type,
			phone = src_phone.phone,
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
