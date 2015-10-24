from sqlalchemy.sql import text

def resetUpdatedFlag( sesTarget, tblName ):
	"""
		Each run of the script requires that the updated_flag field be set to False
		Run a statement at the sql level against the session passed in.
		@Return: no return is required, the session will manage the transaction at
			the database level.
	"""
	sesTarget.execute( text( "UPDATE %s SET updated_flag = :resetFlag" % ( tblName ) ), { "resetFlag" : 0 } )
