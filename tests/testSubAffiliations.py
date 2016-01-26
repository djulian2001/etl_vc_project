import unittest

# the file with the functions we will 'test' this
from bioetl.subAffiliationProcessing import processSubAffiliation

class SubAffiliationProcessingTests( unittest.TestCase ):
	"""Tests for subAffiliationProcessing.py """
	
	def test_processingCondition( self ):
		"""what is this test testing"""
		self.assertTrue( processSubAffiliation( sql_object, sesTgt) )
		
	def test_softDeleteCondition( self ):
		"""what is this test testing"""
		self.assertTrue( softDeleteSubAffiliation( tgtMissingSubAffiliation, srcList ) )

