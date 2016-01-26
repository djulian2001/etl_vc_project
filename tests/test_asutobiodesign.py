import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime, date

from bioetl.models.biopublicmodels import BioPublic, People, PersonWebProfile, Phones
from bioetl.models.asudwpsmodels import AsuDwPsPerson
from bioetl.sharedProcesses import hashThisList
from asutobiodesign_seeds import *

class bioetlTests( unittest.TestCase ):
	"""Tests for subAffiliationProcessing.py """
	def seedPeople( self ):
		for personSeed in peopleSeed:
			personDict = { key: value for key,value in personSeed.iteritems() if key in People.__mapper__.columns.keys() }
			srcHash = hashThisList( personDict.values() )
			personSeedObj = People( **personDict )
			personSeedObj.source_hash = srcHash
			personSeedObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( personSeedObj )
		self.session.commit()

	def seedPhones( self ):
		"""Seed the phone data and dependent persons"""
		self.seedPeople()

		for phoneSeed in phoneSeed:
			srcHash = hashThisList( phoneSeed.values() )
			phoneSeedObj = Phones( **seedPhones )
			getPersonId = self.session.query( People.id ).filter( People.emplid == phoneSeed.emplid ).one()
			phoneSeedObj.person_id = getPersonId.id
			phoneSeedObj.source_hash = srcHash
			phoneSeedObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( phoneSeedObj )
		self.session.commit()

	def seedPersonWeb( self ):
		"""Seed the phone data and dependent persons"""
		self.seedPeople()

		for personWeb in peopleSeed:
			webDict = { key: value for key,value in personWeb.iteritems() if key in PersonWebProfile.__mapper__.columns.keys() }
			srcHash = hashThisList( webDict.values() )
			personWebObj = PersonWebProfile( **webDict )
			getPersonId = self.session.query( People.id ).filter( People.emplid == personWeb['emplid'] ).one()
			personWebObj.person_id = getPersonId.id
			personWebObj.source_hash = srcHash
			personWebObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( personWebObj )
		self.session.commit()

	def setUp( self ):
		"""Need some way of setting up the target database."""
		dbUser = 'app_tester'
		dbPw = 'tannersNeedLove2Plz'
		dbHost = 'dbdev.biodesign.asu.edu'
		dbName	= 'test_bio_public'
		engineString = 'mysql+mysqldb://%s:%s@%s/%s' % ( dbUser, dbPw, dbHost, dbName )
		# self.engine = create_engine( engineString, echo=True )
		self.engine = create_engine( engineString )
		BioPublic.metadata.bind = self.engine
		self.Sessions = scoped_session( sessionmaker( bind=self.engine ) )
		BioPublic.metadata.create_all( self.engine )
		self.session = self.Sessions()

	def tearDown( self ):
		"""These will set up the situation to test the code."""
		self.session.close()
		BioPublic.metadata.drop_all( self.engine )
		self.Sessions.close_all()

	def recordEqualsTest( self, selectObj, seedObj, BaseObj ):
		"""
			DRY... These tests will be run for almost every select object set of tests.
			Using a dictionary comprehension, we will make a test that asserts equality between
			the seed dictionary item and the sqlalchemy object selected from the test database.
		"""
		seedDict = { key: value for key,value in seedObj.iteritems() if key in BaseObj.__mapper__.columns.keys() }
		selectDict = selectObj.__dict__
		for key,value in seedDict.iteritems():
			if not isinstance( selectDict[key], (datetime, date) ):
				self.assertEquals( value , selectDict[key]  )

	def test_false( self ):
		"""Testing False"""
		self.assertFalse( False )

	def test_selectPerson( self ):
		"""The ability to select a record a record out of the database"""
		self.seedPeople()
		records = self.session.query( People ).filter( People.emplid == peopleSeed[0]['emplid'] ).all()
		self.assertIsInstance( records[0], People )
		self.assertEquals( len( records ), 1 )
		self.assertFalse( records[0].updated_flag )
		# self.assertEquals( records[0].emplid, peopleSeed[0]['emplid'] )
		# self.assertEquals( records[0].asurite_id, peopleSeed[0]['asurite_id'] )
		# self.assertEquals( records[0].asu_id, peopleSeed[0]['asu_id'] )
		# self.assertEquals( records[0].ferpa, peopleSeed[0]['ferpa'] )
		# self.assertEquals( records[0].last_name, peopleSeed[0]['last_name'] )
		# self.assertEquals( records[0].first_name, peopleSeed[0]['first_name'] )
		# self.assertEquals( records[0].middle_name, peopleSeed[0]['middle_name'] )
		# self.assertEquals( records[0].display_name, peopleSeed[0]['display_name'] )
		# self.assertEquals( records[0].preferred_first_name, peopleSeed[0]['preferred_first_name'] )
		# self.assertEquals( records[0].affiliations, peopleSeed[0]['affiliations'] )
		# self.assertEquals( records[0].email_address, peopleSeed[0]['email_address'] )
		# self.assertEquals( records[0].eid, peopleSeed[0]['eid'] )
		self.assertIsInstance( records[0].birthdate, date )
		self.assertIsInstance( records[0].created_at, datetime )
		
		self.recordEqualsTest( records[0], peopleSeed[0], People )

	def test_softDeletePerson( self ):
		"""lets test that we can soft delete a person record"""
		from bioetl.personProcessing import softDeletePerson
		self.seedPeople()
		tgtRecords = self.session.query( People ).filter( People.emplid == peopleSeed[0]['emplid'] ).all()
		self.assertEquals( tgtRecords[0].deleted_at, None )
		peopleSeed.pop(0)
		srcObjList = []
		for newSrcObj in peopleSeed:
			srcObjList.append( AsuDwPsPerson( **newSrcObj ) )

		removedRec = softDeletePerson( tgtRecords[0], srcObjList )
		self.assertTrue( removedRec.deleted_at )
		self.assertFalse( removedRec.deleted_at is None )
		self.assertIsInstance( removedRec, People )
		self.assertTrue( removedRec is tgtRecords[0] )

	def test_updatePerson( self ):
		"""The update process for a person"""
		from bioetl.personProcessing import processPerson
		self.seedPeople()
		newEmail = 'primusdj@asu.edu'
		oldEmail = peopleSeed[0]['email_address']
		peopleSeed[0]['email_address'] = newEmail
		newFerpa = 'YES'
		oldFerpa = peopleSeed[0]['ferpa']
		peopleSeed[0]['ferpa'] = newFerpa
		newDisplayName = 'my new name'
		oldDisplayName = peopleSeed[0]['display_name']
		peopleSeed[0]['display_name'] = newDisplayName
		srcPersonObj = AsuDwPsPerson( **peopleSeed[0] )
		record = processPerson( srcPersonObj, self.session )
		self.assertNotEquals( newEmail, oldEmail )
		self.assertNotEquals( newFerpa, oldFerpa )
		self.assertNotEquals( newDisplayName, oldDisplayName )
		self.assertTrue( record.updated_flag )
		self.assertEquals( record.email_address, newEmail )
		self.assertEquals( record.emplid, peopleSeed[0]['emplid'] )
		self.assertEquals( record.asurite_id, peopleSeed[0]['asurite_id'] )
		self.assertEquals( record.asu_id, peopleSeed[0]['asu_id'] )
		self.assertEquals( record.ferpa, newFerpa )
		self.assertEquals( record.last_name, peopleSeed[0]['last_name'] )
		self.assertEquals( record.first_name, peopleSeed[0]['first_name'] )
		self.assertEquals( record.middle_name, peopleSeed[0]['middle_name'] )
		self.assertEquals( record.display_name, newDisplayName )
		self.assertEquals( record.preferred_first_name, peopleSeed[0]['preferred_first_name'] )
		self.assertEquals( record.affiliations, peopleSeed[0]['affiliations'] )
		self.assertEquals( record.eid, peopleSeed[0]['eid'] )

	def test_asuDwPsPerson( self ):
		# from sys import maxint
		bustedSeed = newPersonSeed
		newPersonObj = AsuDwPsPerson( **bustedSeed )
		self.assertEquals( newPersonObj.emplid, newPersonSeed['emplid'] )
		self.assertEquals( newPersonObj.asurite_id, newPersonSeed['asurite_id'] )
		self.assertEquals( newPersonObj.asu_id, newPersonSeed['asu_id'] )
		self.assertEquals( newPersonObj.ferpa, newPersonSeed['ferpa'] )
		self.assertEquals( newPersonObj.last_name, newPersonSeed['last_name'] )
		self.assertEquals( newPersonObj.first_name, newPersonSeed['first_name'] )
		self.assertEquals( newPersonObj.middle_name, newPersonSeed['middle_name'] )
		self.assertEquals( newPersonObj.display_name, newPersonSeed['display_name'] )
		self.assertEquals( newPersonObj.preferred_first_name, newPersonSeed['preferred_first_name'] )
		self.assertEquals( newPersonObj.affiliations, newPersonSeed['affiliations'] )
		self.assertEquals( newPersonObj.email_address, newPersonSeed['email_address'] )
		self.assertEquals( newPersonObj.eid, newPersonSeed['eid'] )
		self.assertEquals( newPersonObj.birthdate, newPersonSeed['birthdate'] )
		self.assertEquals( newPersonObj.last_update, newPersonSeed['last_update'] )
		self.assertEquals( newPersonObj.facebook, newPersonSeed['facebook'] )
		self.assertEquals( newPersonObj.twitter, newPersonSeed['twitter'] )
		self.assertEquals( newPersonObj.google_plus, newPersonSeed['google_plus'] )
		self.assertEquals( newPersonObj.linkedin, newPersonSeed['linkedin'] )
		self.assertEquals( newPersonObj.bio, newPersonSeed['bio'] )
		self.assertEquals( newPersonObj.research_interests, newPersonSeed['research_interests'] )
		self.assertEquals( newPersonObj.cv, newPersonSeed['cv'] )
		self.assertEquals( newPersonObj.website, newPersonSeed['website'] )
		self.assertEquals( newPersonObj.teaching_website, newPersonSeed['teaching_website'] )
		self.assertEquals( newPersonObj.grad_faculties, newPersonSeed['grad_faculties'] )
		self.assertEquals( newPersonObj.professional_associations, newPersonSeed['professional_associations'] )
		self.assertEquals( newPersonObj.work_history, newPersonSeed['work_history'] )
		self.assertEquals( newPersonObj.education, newPersonSeed['education'] )
		self.assertEquals( newPersonObj.research_group, newPersonSeed['research_group'] )
		self.assertEquals( newPersonObj.research_website, newPersonSeed['research_website'] )
		self.assertEquals( newPersonObj.honors_awards, newPersonSeed['honors_awards'] )
		self.assertEquals( newPersonObj.editorships, newPersonSeed['editorships'] )
		self.assertEquals( newPersonObj.presentations, newPersonSeed['presentations'] )

	def test_insertPerson( self ):
		"""Testing that the script will insert a person object."""
		from bioetl.personProcessing import processPerson
		# newPerson = { key : value for key,value in newPersonSeed.iteritems() if key in People.__mapper__.columns.keys() }
		newPersonObj = AsuDwPsPerson( **newPersonSeed )
		records = self.session.query( People ).all()
		self.assertListEqual( records, [] )
		record = processPerson( newPersonObj, self.session )
		self.session.add( record )
		self.session.commit()
		self.assertIsInstance( record, People )
		newRecords = self.session.query( People ).filter( People.emplid == newPersonSeed['emplid'] ).all()
		newRecords = self.session.query( People ).all()
		self.assertTrue( newRecords[0].updated_flag )
		self.assertEquals( newRecords[0].emplid, newPersonSeed['emplid'] )
		self.assertEquals( newRecords[0].asurite_id, newPersonSeed['asurite_id'] )
		self.assertEquals( newRecords[0].asu_id, newPersonSeed['asu_id'] )
		self.assertEquals( newRecords[0].ferpa, newPersonSeed['ferpa'] )
		self.assertEquals( newRecords[0].last_name, newPersonSeed['last_name'] )
		self.assertEquals( newRecords[0].first_name, newPersonSeed['first_name'] )
		self.assertEquals( newRecords[0].middle_name, newPersonSeed['middle_name'] )
		self.assertEquals( newRecords[0].display_name, newPersonSeed['display_name'] )
		self.assertEquals( newRecords[0].preferred_first_name, newPersonSeed['preferred_first_name'] )
		self.assertEquals( newRecords[0].affiliations, newPersonSeed['affiliations'] )
		self.assertEquals( newRecords[0].email_address, newPersonSeed['email_address'] )
		self.assertEquals( newRecords[0].eid, newPersonSeed['eid'] )
		self.assertIsInstance( newRecords[0].birthdate, date )
		self.assertIsInstance( newRecords[0].created_at, datetime )
		with self.assertRaises( ValueError ):
			badSeed = newPersonSeed
			badPersonObj = AsuDwPsPerson( **badSeed )
			badPersonObj.emplid = 2147483647L
			processPerson( badPersonObj, self.session )

	def test_selectPersonWebProfile( self ):
		"""The person web profile 1 to 1 data."""
		self.seedPersonWeb()
		personWebId = self.session.query( People.id ).filter( People.emplid == peopleSeed[1]['emplid'] ).one()
		records = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id == personWebId.id  ).all()
		self.assertIsInstance( records[0], PersonWebProfile )
		self.assertEquals( len( records ), 1 )
		
		# make sure only webprofiles with
		self.recordEqualsTest( records[0], peopleSeed[1], PersonWebProfile )
		

	def test_insertPersonWebProfile( self ):
		"""Test of inserting the person web profiles not all of the records should be added"""
		from bioetl.personWebProfileProcessing import processPersonWebProfile
		self.seedPeople()

		noWebProfile = AsuDwPsPerson( **peopleSeed[0] )
		sessionAction = processPersonWebProfile( noWebProfile, self.session )
		self.assertFalse( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==peopleSeed[0]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertFalse( webProfile )

		yesWebProfile = AsuDwPsPerson( **peopleSeed[1] )
		sessionAction = processPersonWebProfile( yesWebProfile, self.session )
		self.session.add( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==peopleSeed[1]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertIsInstance( sessionAction, PersonWebProfile )
		self.assertEquals( len(webProfile), 1 )
		self.recordEqualsTest( webProfile[0], peopleSeed[1], PersonWebProfile )

		yesWebProfile = AsuDwPsPerson( **peopleSeed[2] )
		sessionAction = processPersonWebProfile( yesWebProfile, self.session )
		self.session.add( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==peopleSeed[2]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertIsInstance( sessionAction, PersonWebProfile )
		self.assertEquals( len(webProfile), 1 )
		self.recordEqualsTest( webProfile[0], peopleSeed[2], PersonWebProfile )


	def test_updatePersonWebProfile( self ):
		from bioetl.personWebProfileProcessing import processPersonWebProfile
		self.seedPersonWeb()
		oldBio = peopleSeed[1]['bio']
		updatePerson = peopleSeed[1]
		newBio = 'blah blah blah blah... im sooo great! blah blah blah'
		
		updatePerson['bio'] = newBio
		
		dwWebProfile = AsuDwPsPerson( **updatePerson )
		sessionAction = processPersonWebProfile( dwWebProfile, self.session )
		self.assertIsInstance( sessionAction, PersonWebProfile )
		self.session.add( sessionAction )

		person = self.session.query( People.id ).filter( People.emplid==peopleSeed[1]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertIsInstance( webProfile[0], PersonWebProfile )
		self.assertEquals( len( webProfile ), 1 )
		self.recordEqualsTest( webProfile[0], updatePerson, PersonWebProfile )

		self.assertNotEquals( webProfile[0].bio, oldBio )
		self.assertEquals( webProfile[0].bio, newBio )



if __name__ == '__main__':
	unittest.main()