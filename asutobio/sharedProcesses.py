###############################################################################
# Utilitie Functions:
###############################################################################
def hashThisList( theList ):
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



def hashThisObject( myObject, myArgs):
	"""[12345,
				566787,
				....]
			
				{
					deptid: 567889
					emplid: 12345
				}"""
	import hashlib

	sss = ''
	for myArg in myArgs:
		thisString += myObject[myArg]
		
	return hashlib.sha256(thisString).hexdigest()