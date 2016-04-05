import unittest
import logging
import copy
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime, date

from bioetl.models.biopublicmodels import BioPublic, People, PersonWebProfile, Phones, Departments, Jobs, JobCodes, JobsLog, SubAffiliations, PersonSubAffiliations
from bioetl.models.asudwpsmodels import AsuDwPsPerson, AsuDwPsPhones, AsuDwPsJobs, AsuDwPsJobsLog, AsuDwPsSubAffiliations
from bioetl.sharedProcesses import hashThisList
from asutobiodesign_seeds import *

class bioetlTests( unittest.TestCase ):
	"""Tests for subAffiliationProcessing.py """
	def seedPeople( self ):
		setPeopleSeeds = copy.deepcopy( peopleSeed )
		for personSeed in setPeopleSeeds:
			personDict = { key: value for key,value in personSeed.iteritems() if key in People.__mapper__.columns.keys() }
			srcHash = hashThisList( personDict.values() )
			personSeedObj = People( **personDict )
			personSeedObj.source_hash = srcHash
			personSeedObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( personSeedObj )

	def seedJobs( self ):
		"""Seed the Jobs"""
		setJobsSeeds = copy.deepcopy(jobsSeed)
		for jobSeed in setJobsSeeds:
			srcHash = hashThisList( jobSeed.values() )
			jobObj = JobCodes( **jobSeed )
			jobObj.source_hash = srcHash
			jobObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( jobObj )

	def seedDepartments( self ):
		"""Seed the Departments"""
		setDeptSeeds = copy.deepcopy( departmentsSeed )
		for deptSeed in setDeptSeeds:
			srcHash = hashThisList( deptSeed.values() )
			deptObj = Departments( **deptSeed )
			deptObj.source_hash = srcHash
			deptObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( deptObj )

	def seedPersonJobLog( self ):
		"""Seed the Job logs table"""
		setJobsLogSeeds = copy.deepcopy( personJobsLogSeed )
		self.seedPeople()
		self.seedDepartments()
		self.seedJobs()
		for jobLogSeed in setJobsLogSeeds:
			srcHash = hashThisList( jobLogSeed.values() )
			jobLogObj = JobsLog( **jobLogSeed )
			jobLogObj.source_hash = srcHash
			person = self.session.query( People.id ).filter( People.emplid==jobLogObj.emplid ).one()
			jobLogObj.person_id = person.id
			dept = self.session.query( Departments.id ).filter( Departments.deptid==jobLogObj.deptid ).one()
			jobLogObj.department_id = dept.id
			job = self.session.query( JobCodes.id ).filter( JobCodes.jobcode==jobLogObj.jobcode ).one()
			jobLogObj.job_id = job.id
			jobLogObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( jobLogObj )

	def seedPersonJobs( self ):
		"""Seed the needed records to work with the personJobs application scripts"""
		setPersonJobSeeds = copy.deepcopy( personJobSeed )
		self.seedPeople()
		self.seedDepartments()
		for jobSeed in setPersonJobSeeds:
			srcHash = hashThisList( jobSeed.values() )
			jobObj = Jobs( **jobSeed )
			jobObj.source_hash = srcHash
			person = self.session.query( People.id ).filter( People.emplid==jobObj.emplid).one()
			jobObj.person_id = person.id
			dept = self.session.query( Departments.id ).filter( Departments.deptid==jobObj.deptid ).one()
			jobObj.department_id = dept.id
			jobObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( jobObj )
	
	def seedPhones( self ):
		"""Seed the phone data and dependent persons"""
		self.seedPeople()
		setPhonesSeeds = copy.deepcopy( phonesSeed )
		for phoneSeed in setPhonesSeeds:
			srcHash = hashThisList( phoneSeed.values() )
			phoneSeedObj = Phones( **phoneSeed )
			getPersonId = self.session.query( People.id ).filter( People.emplid == phoneSeedObj.emplid ).one()
			phoneSeedObj.person_id = getPersonId.id
			phoneSeedObj.source_hash = srcHash
			phoneSeedObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( phoneSeedObj )

	def seedPersonWeb( self ):
		"""Seed the phone data and dependent persons"""
		self.seedPeople()
		setWebProfileSeeds = copy.deepcopy( peopleSeed )
		for personWeb in setWebProfileSeeds:
			webDict = { key: value for key,value in personWeb.iteritems() if key in PersonWebProfile.__mapper__.columns.keys() }
			srcHash = hashThisList( webDict.values() )
			personWebObj = PersonWebProfile( **webDict )
			getPersonId = self.session.query( People.id ).filter( People.emplid == personWeb['emplid'] ).one()
			personWebObj.person_id = getPersonId.id
			personWebObj.source_hash = srcHash
			personWebObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( personWebObj )

	def seedSubAffiliation( self ):
		"""The Sub Affiliation seed process..."""
		from bioetl.models.asudwpsmodels import BiodesignSubAffiliations
		for subAffDict in BiodesignSubAffiliations.seedMe():
			srcHash = hashThisList( subAffDict.values() )
			addObj = SubAffiliations( **subAffDict )
			addObj.source_hash = srcHash
			addObj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			self.session.add( addObj )

	def seedPersonSubAffiliation( self, addNew=False ):
		"""The person Sub Affiliation seed process..."""
		self.seedPeople()
		self.seedDepartments()
		def addThis( seed ):
			"""General add object pattern"""
			srcHash = hashThisList( seed.values() )
			obj = PersonSubAffiliations( **seed )
			obj.source_hash = srcHash
			obj.created_at = datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			person = self.session.query( People.id ).filter( People.emplid==obj.emplid).one()
			obj.person_id = person.id
			dept = self.session.query( Departments.id ).filter( Departments.deptid==obj.deptid ).one()
			obj.department_id = dept.id
			return obj
		
		seeds = copy.deepcopy( personSubAffiliationSeed )
		for seedThis in seeds:
			addObj = addThis( seedThis )
			self.session.add( addObj )

		if addNew:
			addSeeds = copy.deepcopy( newPersonSubAffiliationSeed )
			for addSeed in addSeeds:
				addNewObj = addThis( addSeed )
				self.session.add( addNewObj )

	def setUp( self ):
		"""Need some way of setting up the target database."""
		dbUser = 'app_tester'
		dbPw = 'tannersNeedLove2Plz'
		dbHost = 'dbdev.biodesign.asu.edu'
		dbName	= 'test_bio_public'
		engineString = 'mysql+mysqldb://%s:%s@%s/%s' % ( dbUser, dbPw, dbHost, dbName )
		self.engine = create_engine( engineString )
		BioPublic.metadata.bind = self.engine
		self.Sessions = scoped_session( sessionmaker( bind=self.engine ) )
		BioPublic.metadata.create_all( self.engine )
		self.session = self.Sessions()

		# fakeOracleEngineString = 'sqlite://'
		# self.fakeOraEngine = create_engine( fakeOracleEngineString )
		# self.FakeOraSessions = scoped_session( sessionmaker( bind=self.fakeOraEngine ) )	
		# self.oraSession = self.FakeOraSessions()

	def tearDown( self ):
		"""These will set up the situation to test the code."""
		BioPublic.metadata.drop_all( self.engine )
		self.session.close()
		self.Sessions.close_all()
		self.oraSession.close()
		self.FakeOraSessions.close()

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
		testPeopleSeed = copy.deepcopy( peopleSeed )
		records = self.session.query( People ).filter( People.emplid == testPeopleSeed[0]['emplid'] ).all()
		self.assertIsInstance( records[0], People )
		self.assertEquals( len( records ), 1 )
		self.assertFalse( records[0].updated_flag )
		self.assertIsInstance( records[0].birthdate, date )
		self.assertIsInstance( records[0].created_at, datetime )
		
		self.recordEqualsTest( records[0], testPeopleSeed[0], People )

	def test_softDeletePerson( self ):
		"""lets test that we can soft delete a person record"""
		from bioetl.personProcessing import softDeletePerson
		self.seedPeople()
		testPeopleSeed = copy.deepcopy( peopleSeed )
		tgtRecords = self.session.query( People ).filter( People.emplid == testPeopleSeed[0]['emplid'] ).all()
		self.assertEquals( tgtRecords[0].deleted_at, None )
		testPeopleSeed.pop(0)
		srcObjList = []
		for newSrcObj in testPeopleSeed:
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
		testPeopleSeed = copy.deepcopy( peopleSeed )
		newEmail = 'primusdj@asu.edu'
		oldEmail = testPeopleSeed[0]['email_address']
		testPeopleSeed[0]['email_address'] = newEmail
		newFerpa = 'YES'
		oldFerpa = testPeopleSeed[0]['ferpa']
		testPeopleSeed[0]['ferpa'] = newFerpa
		newDisplayName = 'my new name'
		oldDisplayName = testPeopleSeed[0]['display_name']
		testPeopleSeed[0]['display_name'] = newDisplayName
		srcPersonObj = AsuDwPsPerson( **testPeopleSeed[0] )
		record = processPerson( srcPersonObj, self.session )
		self.assertNotEquals( newEmail, oldEmail )
		self.assertNotEquals( newFerpa, oldFerpa )
		self.assertNotEquals( newDisplayName, oldDisplayName )
		self.assertTrue( record.updated_flag )
		self.assertEquals( record.email_address, newEmail )
		self.assertEquals( record.emplid, testPeopleSeed[0]['emplid'] )
		self.assertEquals( record.asurite_id, testPeopleSeed[0]['asurite_id'] )
		self.assertEquals( record.asu_id, testPeopleSeed[0]['asu_id'] )
		self.assertEquals( record.ferpa, newFerpa )
		self.assertEquals( record.last_name, testPeopleSeed[0]['last_name'] )
		self.assertEquals( record.first_name, testPeopleSeed[0]['first_name'] )
		self.assertEquals( record.middle_name, testPeopleSeed[0]['middle_name'] )
		self.assertEquals( record.display_name, newDisplayName )
		self.assertEquals( record.preferred_first_name, testPeopleSeed[0]['preferred_first_name'] )
		self.assertEquals( record.affiliations, testPeopleSeed[0]['affiliations'] )
		self.assertEquals( record.eid, testPeopleSeed[0]['eid'] )

	def test_asuDwPsPerson( self ):
		# from sys import maxint
		testNewPersonSeed = copy.deepcopy( newPersonSeed )
		bustedSeed = copy.deepcopy( newPersonSeed )
		newPersonObj = AsuDwPsPerson( **bustedSeed )
		self.assertEquals( newPersonObj.emplid, testNewPersonSeed['emplid'] )
		self.assertEquals( newPersonObj.asurite_id, testNewPersonSeed['asurite_id'] )
		self.assertEquals( newPersonObj.asu_id, testNewPersonSeed['asu_id'] )
		self.assertEquals( newPersonObj.ferpa, testNewPersonSeed['ferpa'] )
		self.assertEquals( newPersonObj.last_name, testNewPersonSeed['last_name'] )
		self.assertEquals( newPersonObj.first_name, testNewPersonSeed['first_name'] )
		self.assertEquals( newPersonObj.middle_name, testNewPersonSeed['middle_name'] )
		self.assertEquals( newPersonObj.display_name, testNewPersonSeed['display_name'] )
		self.assertEquals( newPersonObj.preferred_first_name, testNewPersonSeed['preferred_first_name'] )
		self.assertEquals( newPersonObj.affiliations, testNewPersonSeed['affiliations'] )
		self.assertEquals( newPersonObj.email_address, testNewPersonSeed['email_address'] )
		self.assertEquals( newPersonObj.eid, testNewPersonSeed['eid'] )
		self.assertEquals( newPersonObj.birthdate, testNewPersonSeed['birthdate'] )
		self.assertEquals( newPersonObj.last_update, testNewPersonSeed['last_update'] )
		self.assertEquals( newPersonObj.facebook, testNewPersonSeed['facebook'] )
		self.assertEquals( newPersonObj.twitter, testNewPersonSeed['twitter'] )
		self.assertEquals( newPersonObj.google_plus, testNewPersonSeed['google_plus'] )
		self.assertEquals( newPersonObj.linkedin, testNewPersonSeed['linkedin'] )
		self.assertEquals( newPersonObj.bio, testNewPersonSeed['bio'] )
		self.assertEquals( newPersonObj.research_interests, testNewPersonSeed['research_interests'] )
		self.assertEquals( newPersonObj.cv, testNewPersonSeed['cv'] )
		self.assertEquals( newPersonObj.website, testNewPersonSeed['website'] )
		self.assertEquals( newPersonObj.teaching_website, testNewPersonSeed['teaching_website'] )
		self.assertEquals( newPersonObj.grad_faculties, testNewPersonSeed['grad_faculties'] )
		self.assertEquals( newPersonObj.professional_associations, testNewPersonSeed['professional_associations'] )
		self.assertEquals( newPersonObj.work_history, testNewPersonSeed['work_history'] )
		self.assertEquals( newPersonObj.education, testNewPersonSeed['education'] )
		self.assertEquals( newPersonObj.research_group, testNewPersonSeed['research_group'] )
		self.assertEquals( newPersonObj.research_website, testNewPersonSeed['research_website'] )
		self.assertEquals( newPersonObj.honors_awards, testNewPersonSeed['honors_awards'] )
		self.assertEquals( newPersonObj.editorships, testNewPersonSeed['editorships'] )
		self.assertEquals( newPersonObj.presentations, testNewPersonSeed['presentations'] )

	def test_insertPerson( self ):
		"""Testing that the script will insert a person object."""
		from bioetl.personProcessing import processPerson
		testNewPerson = copy.deepcopy( newPersonSeed )
		
		newPersonObj = AsuDwPsPerson( **testNewPerson )
		records = self.session.query( People ).all()
		self.assertListEqual( records, [] )
		record = processPerson( newPersonObj, self.session )
		self.session.add( record )
		self.session.commit()
		self.assertIsInstance( record, People )
		newRecords = self.session.query( People ).filter( People.emplid == testNewPerson['emplid'] ).all()
		newRecords = self.session.query( People ).all()
		self.assertTrue( newRecords[0].updated_flag )
		self.recordEqualsTest( newPersonObj, testNewPerson, People )
		
		self.assertEquals( newRecords[0].emplid, testNewPerson['emplid'] )
		self.assertEquals( newRecords[0].asurite_id, testNewPerson['asurite_id'] )
		self.assertEquals( newRecords[0].asu_id, testNewPerson['asu_id'] )
		self.assertEquals( newRecords[0].ferpa, testNewPerson['ferpa'] )
		self.assertEquals( newRecords[0].last_name, testNewPerson['last_name'] )
		self.assertEquals( newRecords[0].first_name, testNewPerson['first_name'] )
		self.assertEquals( newRecords[0].middle_name, testNewPerson['middle_name'] )
		self.assertEquals( newRecords[0].display_name, testNewPerson['display_name'] )
		self.assertEquals( newRecords[0].preferred_first_name, testNewPerson['preferred_first_name'] )
		self.assertEquals( newRecords[0].affiliations, testNewPerson['affiliations'] )
		self.assertEquals( newRecords[0].email_address, testNewPerson['email_address'] )
		self.assertEquals( newRecords[0].eid, testNewPerson['eid'] )

		self.assertIsInstance( newRecords[0].birthdate, date )
		self.assertIsInstance( newRecords[0].created_at, datetime )
		with self.assertRaises( ValueError ):
			badSeed = testNewPerson
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

		testPeopleSeed = copy.deepcopy( peopleSeed )

		noWebProfile = AsuDwPsPerson( **testPeopleSeed[0] )
		sessionAction = processPersonWebProfile( noWebProfile, self.session )
		self.assertFalse( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==testPeopleSeed[0]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertFalse( webProfile )

		yesWebProfile = AsuDwPsPerson( **testPeopleSeed[1] )
		sessionAction = processPersonWebProfile( yesWebProfile, self.session )
		self.session.add( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==testPeopleSeed[1]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertIsInstance( sessionAction, PersonWebProfile )
		self.assertEquals( len(webProfile), 1 )
		self.recordEqualsTest( webProfile[0], testPeopleSeed[1], PersonWebProfile )

		yesWebProfile = AsuDwPsPerson( **testPeopleSeed[2] )
		sessionAction = processPersonWebProfile( yesWebProfile, self.session )
		self.session.add( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==testPeopleSeed[2]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertIsInstance( sessionAction, PersonWebProfile )
		self.assertEquals( len(webProfile), 1 )
		self.recordEqualsTest( webProfile[0], testPeopleSeed[2], PersonWebProfile )

	def test_updatePersonWebProfile( self ):
		from bioetl.personWebProfileProcessing import processPersonWebProfile
		self.seedPersonWeb()
		testPeopleSeed = copy.deepcopy( peopleSeed )
		newBio = 'blah blah blah blah... im sooo great! blah blah blah'
		oldBio = testPeopleSeed[1]['bio']
		testPeopleSeed[1]['bio'] = newBio
		dwWebProfile = AsuDwPsPerson( **testPeopleSeed[1] )
		sessionAction = processPersonWebProfile( dwWebProfile, self.session )
		self.assertIsInstance( sessionAction, PersonWebProfile )
		self.session.add( sessionAction )
		person = self.session.query( People.id ).filter( People.emplid==testPeopleSeed[1]['emplid'] ).one()
		webProfile = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id==person.id ).all()
		self.assertIsInstance( webProfile[0], PersonWebProfile )
		self.assertEquals( len( webProfile ), 1 )
		self.recordEqualsTest( webProfile[0], testPeopleSeed[1], PersonWebProfile )
		self.assertNotEquals( webProfile[0].bio, oldBio )
		self.assertEquals( webProfile[0].bio, newBio )

	def test_softDeletePersonWebProfile( self ):
		from bioetl.personWebProfileProcessing import softDeletePersonWebProfile
		self.seedPersonWeb()
		testPeopleSeed = copy.deepcopy( peopleSeed )
		person = self.session.query( People.id ).filter( People.emplid == testPeopleSeed[0]['emplid'] ).all()
		tgtRecords = self.session.query( PersonWebProfile ).filter( PersonWebProfile.person_id == person[0].id ).all()
		self.assertEquals( len( tgtRecords ), 1 )
		self.assertEquals( tgtRecords[0].deleted_at, None )
		testPeopleSeed.pop(0)
		srcObjList = []
		for newSrcObj in testPeopleSeed:
			srcObjList.append( AsuDwPsPerson( **newSrcObj ) )

		removedRec = softDeletePersonWebProfile( tgtRecords[0], srcObjList )
		self.assertTrue( removedRec.deleted_at )
		self.assertFalse( removedRec.deleted_at is None )
		self.assertIsInstance( removedRec, PersonWebProfile )
		self.assertTrue( removedRec is tgtRecords[0] )

	def test_selectPersonPhones( self ):
		"""The phones represent the application handling of zero to many relationships."""
		self.seedPhones()
		testPeople = copy.deepcopy( peopleSeed ) # Brian
		testPhones = copy.deepcopy( phonesSeed )
		for testPhone in testPhones:
			person = self.session.query( People ).filter( People.emplid==testPeople[2]['emplid'] ).one()
			tgtRecords = self.session.query( Phones ).filter( Phones.person_id==person.id ).all()
			self.assertEquals( len( tgtRecords ), 3 )
			for tgtRecord in tgtRecords:
				index = next( x for x in testPhones if x['phone_type']==tgtRecord.phone_type and x['phone']==tgtRecord.phone )
				self.recordEqualsTest( tgtRecord, index, Phones )

	def test_updateOnePersonPhone( self ):
		"""
			The phone updates represent a core logic of the application.  This tests that the update of
			one phone number will be caught and the change made.
		"""
		from bioetl.personPhoneProcessing import processPhone
		self.seedPhones()
		testPhones = copy.deepcopy( phonesSeed )
		oldPhone = testPhones[0]['phone']
		newPhone = '3334445555'
		testPhones[0]['phone'] = newPhone
		testObj = AsuDwPsPhones( **testPhones[0] )
		testResult = processPhone( testObj, self.session )
		getMeBack = self.session.query( Phones ).filter( Phones.emplid==testObj.emplid ).filter( Phones.phone==testObj.phone ).filter( Phones.phone_type==testObj.phone_type ).all()
		self.assertIs( getMeBack[0], testResult )
		self.assertIsInstance( testResult, Phones )
		self.assertNotEquals( testResult.phone, oldPhone )
		self.assertEquals( testResult.phone, newPhone )
		self.assertEquals( testResult.phone, testPhones[0]['phone'] )
		self.assertEquals( testResult.phone_type, testPhones[0]['phone_type'] )

	def test_updateManyPersonPhone( self ):
		"""
			This test looks into the applications handling of many records associated to a person record
			where differing between unique records would be difficult
		"""
		from bioetl.personPhoneProcessing import processPhone
		self.seedPhones()
		testPhones = copy.deepcopy( phonesSeed )
		updateMe = testPhones[4]
		noChanges = testPhones[3]
		oldPhone = updateMe['phone']
		newPhone = "3334445555"
		updateMe['phone'] = newPhone

		self.assertEquals( updateMe['phone_type'], noChanges['phone_type'] )

		testUpdateObj = AsuDwPsPhones( **updateMe )
		testUpdateResults = processPhone( testUpdateObj, self.session )

		getMeBack = self.session.query( Phones ).filter( Phones.emplid==testUpdateObj.emplid ).filter( Phones.phone==testUpdateObj.phone ).filter( Phones.phone_type==testUpdateObj.phone_type ).all()
		self.assertIs( getMeBack[0], testUpdateResults )
		self.recordEqualsTest( getMeBack[0], updateMe, Phones )
		self.assertIsInstance( testUpdateResults, Phones )
		self.assertNotEquals( testUpdateResults.phone, oldPhone )
		self.assertTrue( testUpdateResults.updated_flag )
		self.assertEquals( testUpdateResults.phone, newPhone )
		self.assertEquals( testUpdateResults.emplid, updateMe['emplid'] )
		self.assertEquals( testUpdateResults.phone, updateMe['phone'] )
		self.assertEquals( testUpdateResults.phone_type, updateMe['phone_type'] )
		
		testThis = self.session.query( Phones ).filter( Phones.emplid==noChanges['emplid'] ).filter( Phones.updated_flag==False ).filter( Phones.phone_type==noChanges['phone_type'] ).all()
		self.assertEquals( len(testThis),1 )
		self.assertFalse( testThis[0].updated_flag )
		self.assertNotEquals( testThis[0].phone, noChanges['phone'] )

		testNoChangeObj = AsuDwPsPhones( **noChanges )
		testNoChangeResults = processPhone( testNoChangeObj, self.session )

		getUpdateBack = self.session.query( Phones ).filter( Phones.emplid==testNoChangeResults.emplid ).filter( Phones.phone==testNoChangeResults.phone ).filter( Phones.phone_type==testNoChangeResults.phone_type ).all()
		self.assertIs( getUpdateBack[0], testNoChangeResults )
		self.assertIsInstance( testNoChangeResults, Phones )
		self.assertTrue( testNoChangeResults.updated_flag )
		self.assertEquals( testNoChangeResults.emplid, noChanges['emplid'] )
		self.assertEquals( testNoChangeResults.phone, noChanges['phone'] )
		self.assertEquals( testNoChangeResults.phone_type, noChanges['phone_type'] )
		
		testUpdates = self.session.query( Phones ).filter( Phones.emplid==noChanges['emplid'] ).filter( Phones.updated_flag==True ).filter( Phones.phone_type==noChanges['phone_type'] ).all()
		self.assertEquals( len( testUpdates ),2 )
		
	def test_insertNewPhone( self ):
		"""Test that the application script to insert a new phone, does just that."""
		self.seedPeople()
		testPhones = copy.deepcopy( phonesSeed )
		from bioetl.personPhoneProcessing import processPhone

		for testPhone in testPhones:
			newPhoneObj = AsuDwPsPhones( **testPhone )
			phoneResult = processPhone( newPhoneObj, self.session )
			self.assertIsInstance( phoneResult, Phones )
			self.assertTrue( phoneResult.updated_flag )
			self.recordEqualsTest( phoneResult, testPhone, Phones )

			self.session.add( phoneResult )
			selectMe = self.session.query( Phones ).filter( Phones.emplid==phoneResult.emplid ).filter( Phones.source_hash == phoneResult.source_hash ).all()
			self.assertIs( selectMe[0], phoneResult )

		newPhone = copy.deepcopy( newPhoneSeed )
		PhoneObj = AsuDwPsPhones( **newPhone )
		phoneResult = processPhone( PhoneObj, self.session )

		self.assertIsInstance( phoneResult, Phones )
		self.assertTrue( phoneResult.updated_flag )
		self.recordEqualsTest( phoneResult, newPhone, Phones )
		
		self.session.add( phoneResult )
		selectMe = self.session.query( Phones ).filter( Phones.emplid==phoneResult.emplid ).filter( Phones.source_hash == phoneResult.source_hash ).all()
		self.assertIs( selectMe[0], phoneResult )

	def test_selectPersonJobs( self ):
		"""Another aspect of this application is the one to many to many, or relationship table model."""
		self.seedPersonJobs()
		testJobs = copy.deepcopy( personJobSeed )

		testResult = self.session.query( Jobs ).filter( Jobs.emplid==testJobs[0]['emplid'] ).all()
		self.recordEqualsTest( testResult[0], testJobs[0], Jobs )

	def test_updatePersonJobsDepartment( self ):
		"""When a part of a records uniqueness is changed, the record will look as if it's new"""
		from bioetl.personJobsProcessing import processPersonJob
		self.seedPersonJobs()
		testJobs = copy.deepcopy( personJobSeed )
		oldDeptid = testJobs[0]['deptid']
		testResult = self.session.query( Jobs ).filter( Jobs.emplid==testJobs[0]['emplid'] ).all()
		self.recordEqualsTest( testResult[0], testJobs[0], Jobs )
		newDeptid = 'E0802005'
		testJobs[0]['deptid'] = newDeptid
		self.assertNotEquals( testResult[0].deptid, testJobs[0]['deptid'] )
		newUpdateObj = AsuDwPsJobs( **testJobs[0] )
		appAction = processPersonJob( newUpdateObj, self.session )
		self.assertIsInstance( appAction, Jobs )
		self.session.add( appAction )
		returnRecords = self.session.query( Jobs ).all()
		self.assertNotEquals( len( returnRecords ), 3 )
		self.assertEquals( len( returnRecords ), 4 )

	def test_updatePersonJobsTitle( self ):
		"""When a part of a records uniqueness is changed, the record will look as if it's new"""
		from bioetl.personJobsProcessing import processPersonJob
		self.seedPersonJobs()
		testJobs = copy.deepcopy( personJobSeed )
		oldDeptid = testJobs[0]['title']
		testResult = self.session.query( Jobs ).filter( Jobs.emplid==testJobs[0]['emplid'] ).all()
		self.recordEqualsTest( testResult[0], testJobs[0], Jobs )
		newDeptid = 'hehe-haha expert'
		testJobs[0]['title'] = newDeptid
		self.assertNotEquals( testResult[0].title, testJobs[0]['title'] )
		newUpdateObj = AsuDwPsJobs( **testJobs[0] )
		appAction = processPersonJob( newUpdateObj, self.session )
		self.assertIsInstance( appAction, Jobs )
		self.session.add( appAction )
		returnRecords = self.session.query( Jobs ).all()
		self.assertNotEquals( len( returnRecords ), 3 )
		self.assertEquals( len( returnRecords ), 4 )

	def test_softDeletePersonJobsAfterUpdate( self ):
		"""As reflected in other tests of the personJobs processing, updates to the compound keys will
		require an insert then a softdelete / delete of the old record.  lets make sure that works."""
		from bioetl.personJobsProcessing import processPersonJob, softDeletePersonJob
		self.seedPersonJobs()
		testJobs = copy.deepcopy( personJobSeed )
		newDeptid = 'hehe-haha expert'
		testJobs[0]['title'] = newDeptid
		newUpdateObj = AsuDwPsJobs( **testJobs[0] )
		appAction = processPersonJob( newUpdateObj, self.session )
		self.assertIsInstance( appAction, Jobs )
		self.session.add( appAction )
		returnRecords = self.session.query( Jobs ).filter( Jobs.deleted_at==None ).all()
		self.assertNotEquals( len( returnRecords ), 3 )
		self.assertEquals( len( returnRecords ), 4 )
		fakeSourceList = [ AsuDwPsJobs( **testJob ) for testJob in testJobs ]

		for tgtRecord in returnRecords:
			removeMe = softDeletePersonJob( tgtRecord, fakeSourceList )
			if removeMe:
				self.session.add( removeMe )

		myReturnRecords = self.session.query( Jobs ).filter( Jobs.deleted_at==None ).all()
		self.assertNotEquals( len( myReturnRecords ), 4 )
		self.assertEquals( len( myReturnRecords ), 3 )
		
	def test_selectPersonSubAffiliation( self ):
		"""The Sub Affiliations are an important part of biodesign."""
		self.seedPersonSubAffiliation()

		seed = copy.deepcopy( personSubAffiliationSeed )
		subAffObjs = self.session.query( PersonSubAffiliations ).filter( PersonSubAffiliations.emplid == seed[0]['emplid'] ).all()

		for subAffObj in subAffObjs:
			self.recordEqualsTest( subAffObj, seed[0], PersonSubAffiliations )

	def test_insertPersonSubAffiliation( self ):
		"""Testing that new Sub Affiliations can be added"""
		from bioetl.personSubAffiliationProcessing import processPersonSubAffiliation
		self.seedPersonSubAffiliation()
		seeds = copy.deepcopy( newPersonSubAffiliationSeed )
		
		for seed in seeds:
			subAffObj = AsuDwPsSubAffiliations( **seed )
			appResult = processPersonSubAffiliation( subAffObj, self.session )
			self.session.add( appResult )
			appResultReturned = self.session.query( PersonSubAffiliations ).filter( 
				PersonSubAffiliations.emplid == seed['emplid'] ).filter(
				PersonSubAffiliations.deptid == seed['deptid'] ).filter(
				PersonSubAffiliations.subaffiliation_code == seed['subaffiliation_code'] ).filter(
				PersonSubAffiliations.updated_flag == True ).all()
			self.recordEqualsTest( appResultReturned[0], seed, PersonSubAffiliations )

	def test_updatePersonSubAffiliation( self ):
		"""Testing that the person sub affiliations can be updated, single record person sub affiliation"""
		from bioetl.personSubAffiliationProcessing import processPersonSubAffiliation
		self.seedPersonSubAffiliation()
		mySeed = copy.deepcopy( personSubAffiliationSeed[2] )
		oldTitle = mySeed['title']
		newTitle = 'Do we have to?'
		mySeed['title'] = newTitle
		result = processPersonSubAffiliation( AsuDwPsSubAffiliations( **mySeed ), self.session )
		self.assertIsInstance( result, PersonSubAffiliations )
		self.recordEqualsTest( result, mySeed, PersonSubAffiliations )
		self.assertNotEquals( result.title, oldTitle )

	def test_insertDuplicatesPersonSubAffiliation( self ):
		"""Testing the person sub affiliations, can duplicates be added"""
		from bioetl.personSubAffiliationProcessing import processPersonSubAffiliation
		def getAllRecords():
			return self.session.query( PersonSubAffiliations ).all()

		self.seedPersonSubAffiliation()
		addSeeds = copy.deepcopy( dubsPersonSubAffiliationSeed )
		
		startingResults = getAllRecords()
		recCount = len( startingResults ) 

		for addSeed in addSeeds:
			aSeedObj = AsuDwPsSubAffiliations( **addSeed )
			anAction = processPersonSubAffiliation( aSeedObj, self.session )
			self.session.add( anAction )
			recCount += 1

		endingResults = getAllRecords()
		# the added seeds has a duplicated record, which should create the same source_hash and reject the value...
		self.assertEquals( len( endingResults ), recCount )

	def test_updateManyToManyPersonSubAffiliation( self ):
		"""Testing the person sub affiliations, updates to a person object with many sub affiliations"""
		from bioetl.personSubAffiliationProcessing import processPersonSubAffiliation
		self.seedPersonSubAffiliation( True )
		
		editSeeds = copy.deepcopy( newPersonSubAffiliationSeed )
		oldTitle = editSeeds[1]['title']
		newTitle = 'Bring Dale back...'
		updateObj = AsuDwPsSubAffiliations( **editSeeds[1] )
		updateObj.title = newTitle

		preValues = self.session.query(
			PersonSubAffiliations ).filter(
				PersonSubAffiliations.emplid == editSeeds[1]['emplid'] ).all()

		self.assertEquals( len( preValues ), 3 )

		anAction = processPersonSubAffiliation( updateObj, self.session )
		self.session.add(anAction)
		self.assertIsInstance( anAction, PersonSubAffiliations )
		resultsTest1 = self.session.query( 
			PersonSubAffiliations ).filter( 
				PersonSubAffiliations.emplid == editSeeds[1]['emplid'] ).all()

		self.assertEquals( len( resultsTest1 ), 3 )
		resultsTest2 = self.session.query( 
			PersonSubAffiliations ).filter( 
				PersonSubAffiliations.emplid == editSeeds[1]['emplid'] ).filter(
				PersonSubAffiliations.updated_flag == True ).all()

		self.assertEquals( len( resultsTest2 ), 1 )
		self.assertIsInstance( resultsTest2[0], PersonSubAffiliations )
		self.assertNotEquals( oldTitle, resultsTest2[0].title )

		editSeeds2 = copy.deepcopy( personSubAffiliationSeed )
		oldTitle2 = editSeeds2[2]['title']
		newTitle2 = 'Bring him back...'
		updateObj2 = AsuDwPsSubAffiliations( **editSeeds2[2] )
		updateObj2.title = newTitle2
		anAction2 = processPersonSubAffiliation( updateObj2, self.session )
		self.session.add(anAction2)
		self.assertIsInstance( anAction2, PersonSubAffiliations )
		resultsTest3 = self.session.query( 
			PersonSubAffiliations ).filter( 
				PersonSubAffiliations.emplid == editSeeds2[2]['emplid'] ).all()

		self.assertEquals( len( resultsTest3 ), 3 )
		resultsTest4 = self.session.query( 
			PersonSubAffiliations ).filter( 
				PersonSubAffiliations.emplid == editSeeds2[2]['emplid'] ).filter(
				PersonSubAffiliations.updated_flag == True ).all()

		self.assertEquals( len( resultsTest4 ), 2 )
		self.assertIsInstance( resultsTest4[0], PersonSubAffiliations )
		self.assertNotEquals( oldTitle2, resultsTest4[0].title )
		update2Found = self.session.query(
			PersonSubAffiliations ).filter(
				PersonSubAffiliations.emplid == editSeeds2[2]['emplid'] ).filter(
				PersonSubAffiliations.title == newTitle2 ).filter(
				PersonSubAffiliations.updated_flag == True ).all()

		self.assertEquals( newTitle2, update2Found[0].title )

	def test_updatePersonJobsLog( self ):
		from bioetl.jobLogProcessing import processJobLog
		self.seedPersonJobLog()
		mySeed = copy.deepcopy( personJobsLogSeed[3] )
		oldEffdt = mySeed['effdt']
		newEffdt = '2008-01-30'
		mySeed['effdt'] = newEffdt

		preCount = self.session.query( JobsLog ).filter( JobsLog.emplid == mySeed['emplid'] ).all()

		result = processJobLog( AsuDwPsJobsLog( **mySeed ), self.session )
		self.assertIsInstance( result, JobsLog )
		self.recordEqualsTest( result, mySeed, JobsLog )
		self.assertNotEquals( result.effdt, oldEffdt )

		postCount = self.session.query( JobsLog ).filter( JobsLog.emplid == mySeed['emplid'] ).all()
		self.assertEquals( len(preCount), len(postCount) )

	def test_insertPersonJobsLog( self ):
		from bioetl.jobLogProcessing import processJobLog
		self.seedPersonJobLog()
		newSeed = copy.deepcopy( newPersonJobsLogSeed )
		
		preCount = self.session.query( JobsLog ).all()

		newResult = processJobLog( AsuDwPsJobsLog( **newSeed ), self.session )
		self.assertIsInstance( newResult, JobsLog )
		self.recordEqualsTest( newResult, newSeed, JobsLog )
		self.session.add( newResult )

		postCount = self.session.query( JobsLog ).all()
		self.assertNotEquals( len( preCount ),len( postCount ) )

	def test_insertDubPersonJobsLog( self ):
		from bioetl.jobLogProcessing import processJobLog
		self.seedPersonJobLog()
		newSeed = copy.deepcopy( dubPersonJobsLogSeed )
		
		preCount = self.session.query( JobsLog ).all()

		newResult = processJobLog( AsuDwPsJobsLog( **newSeed ), self.session )
		self.assertIsInstance( newResult, JobsLog )
		self.recordEqualsTest( newResult, newSeed, JobsLog )
		self.session.add( newResult )

		postCount = self.session.query( JobsLog ).all()
		self.assertEquals( len( preCount ),len( postCount ) )

	def test_noPersonMatchForJobsLog( self ):
		from bioetl.jobLogProcessing import processJobLog
		self.seedPersonJobLog()
		noneSeed = copy.deepcopy( nonePersonJobsLogSeed )
		with self.assertRaises( NoResultFound ):
			noneJobLogObj = AsuDwPsJobsLog( **noneSeed )
			noneResult = processJobLog( noneJobLogObj, self.session )

	def test_initOfEtlProcess( self ):
		"""Test that an etl process can be intaintiated, 
			args:
				sessions -> @src & @tgt
				
				logging? -> pass in name of log?
				missing data (emplid)? -> just initiated...
				scoped method runs, factory? (admin, publications, etc...)?


		"""
		from bioetl.etlProcess import EtlProcess
		# model the def __main__():
		etl = EtlProcess()
		
		self.assertIsInstance( etl, EtlProcess )
		self.assertIsInstance( etl.missingemplid, list )
		self.assertIsNotNone( etl.missingemplid )
		self.assertFalse( etl.missingemplid )
		



if __name__ == '__main__':
	unittest.main()