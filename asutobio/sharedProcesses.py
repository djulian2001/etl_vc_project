###############################################################################
# Utilitie Functions:
###############################################################################
def hashThisList(theList):
	"""
		The following takes in a list of variable data types, casts them to a
		string, then concatenates them together, then hashs the string value
		and returns it.
	"""
	import hashlib
	
	thisString = ""
	for i in theList:
		thisString += str(i)

	thisSha256Hash = hashlib.sha256(thisString).hexdigest()

	return thisSha256Hash

