from sqlalchemy.sql import text
import hashlib


def resetUpdatedFlag( sesTarget, tblName ):
	"""
		Each run of the script requires that the updated_flag field be set to False
		Run a statement at the sql level against the session passed in.
		@Return: no return is required, the session will manage the transaction at
			the database level.
	"""
	sesTarget.execute( text( "UPDATE %s SET updated_flag = :resetFlag" % ( tblName ) ), { "resetFlag" : 0 } )


###############################################################################
# Utilitie Functions:
###############################################################################
def hashThisList( theList ):
	"""
		The following takes in a list of variable data types, casts them to a
		string, then concatenates them together, then hashs the string value
		and returns it.
	"""
	
	thisString = ""
	for i in theList:
		thisString += str( i )

	thisSha256Hash = hashlib.sha256(thisString).hexdigest()

	return thisSha256Hash

# def hashThisObject( myObject, myArgs):
# 	""" THIS DOES NOT Work..."""
# 	import hashlib

# 	thisString = ""
# 	for myArg in myArgs:
# 		thisString += str( myObject[myArg] )
		
# 	return hashlib.sha256(thisString).hexdigest()

