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

src_people = src_session.query(PsPeople).all()

i = 1
for src_person in src_people:
	# determine the person exists in the target database.
	(person_exists, ), = TgtSession.query(exists().where(People.emplid==src_person.emplid))
	if person_exists == True:
		# determine if the person that exists requires an update.
		(no_update_required, ), = TgtSession.query(
			exists().where(
				People.emplid==src_person.emplid
			).where(
				People.source_hash==src_person.source_hash
			)
		)
		if no_update_required == True:
			# print i
			pass
		else:
			# update the the database with the data changes.
			update_person = tgt_session.query(People).filter(People.emplid==src_person.emplid).one()

			update_person.source_hash = src_person.source_hash
			update_person.emplid = src_person.emplid
			update_person.asurite_id = src_person.asurite_id
			update_person.asu_id = src_person.asu_id
			update_person.ferpa = src_person.ferpa
			update_person.last_name = src_person.last_name
			update_person.first_name = src_person.first_name
			update_person.middle_name = src_person.middle_name
			update_person.display_name = src_person.display_name
			update_person.preferred_first_name = src_person.preferred_first_name
			update_person.affiliations = src_person.affiliations
			update_person.email_address = src_person.email_address
			update_person.eid = src_person.eid
			update_person.birthdate = src_person.birthdate
			update_person.updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

			print "updated_at:\t" + update_person.updated_at

			tgt_session.add(update_person)
			# print i
			
			# tgt_person = People()
	else:
		# person wasn't in the target databases, add them now
		tgt_person = People(
			source_hash = src_person.source_hash,
			emplid = src_person.emplid,
			asurite_id = src_person.asurite_id,
			asu_id = src_person.asu_id,
			ferpa = src_person.ferpa,
			last_name = src_person.last_name,
			first_name = src_person.first_name,
			middle_name = src_person.middle_name,
			display_name = src_person.display_name,
			preferred_first_name = src_person.preferred_first_name,
			affiliations = src_person.affiliations,
			email_address = src_person.email_address,
			eid = src_person.eid,
			birthdate = src_person.birthdate,
			created_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
			# last_update = src_person.last_update
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

	# tgt_session_people.query(or_(
	# 	exists().select_from
	# 	People).filter(People.emplid==src_person.emplid)
	# 	)
		

# 	print person
# print people

# s_people = select([people]).where(people.c.first_name.)

# # print s_people
# result = source_conn.execute(s_people)

# print result