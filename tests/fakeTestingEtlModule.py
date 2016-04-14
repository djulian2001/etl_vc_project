
def getTableName():
	"""Fake"""
	return 'fakeTableName'

def getSourceData( sesSource, qryList=None ):
	"""Fake"""
	if not qryList:
		return "subquery mode"
	else:
		return qryList

def processData( srcPerson, sesTarget ):
	"""Fake"""
	return True

def getTargetData( sesTarget ):
	"""Fake"""
	return True


def softDeleteData( tgtRecord, srcRecords ):
	"""Fake"""
	return True
