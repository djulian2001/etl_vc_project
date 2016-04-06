from sharedProcesses import resetUpdatedFlag

import logging

logger = logging.getLogger( __name__ )

class ModuleProcessController( object ):
	"""
		There is a very common pattern that is used to process almost every module 
		imported to process those specific data sets.  This is a class that will
		move all of those patterns into a single point.

	"""
	def __init__( self, aModule, sesTarget, sesSource=None ):
		"""Force the set/get variables force the state of the controller"""
		self.module = aModule
		self.sesTarget = sesTarget
		self.sesSource = sesSource
		self.cacheSource = None
		self.overrideSource = None
		self.queryByList = None
		self.tablename = None

	def processSource( self ):
		"""
			Might be that there is no need for this method, just to move this process
			into the init method, but it seems wiser to trigger the events, or not we
	  	"""
		self.tablename = self.module.getTableName()

		resetUpdatedFlag( self.sesTarget, self.tablename )
	
		if not self.overrideSource:
			srcDataResults = self.module.getSourceData( self.sesSource, self.queryByList if self.queryByList else None ) 
		else:
			srcDataResults = self.overrideSource

		if not self.queryByList:
			self.cacheSource = srcDataResults
		
		iRecords = 1
		for srcData in srcDataResults:
			processedSrcData = self.module.processData( srcData, self.sesTarget )
			if processedSrcData:
				self.sesTarget.add( processedSrcData )

				if iRecords % 1000 == 0:
					try:
						self.sesTarget.flush()
					except sqlalchemy.exc.IntegrityError as e:
						self.sesTarget.rollback()
						raise e
				iRecords += 1

		self.commitThis()
		resetUpdatedFlag( self.sesTarget, self.tablename )
		self.commitThis()
		logger.info("{} source records processed: {} of {} records.".format( self.tablename, iRecords-1, len( srcDataResults ) ) )

	def cleanTarget( self ):
		"""Soft delete the data thats no longer found int the source db"""
		tgtDataRemovals = self.module.getTargetData( self.sesTarget )
		iRecords = 1
		for dataRemoval in tgtDataRemovals:
			processedDataRemoval = self.module.softDeleteData( dataRemoval, self.cacheSource )

			if processedDataRemoval:
				self.sesTarget.add( processedDataRemoval )

				if iRecords % 1000 == 0:
					try:
						self.sesTarget.flush()
					except sqlalchemy.exc.IntegrityError as e:
						self.sesTarget.rollback()
						raise e
				iRecords += 1

		self.commitThis()

		logger.info("{} target records removed: {} of {} records".format( self.tablename, iRecords-1, len( tgtDataRemovals ) ) )


	def commitThis( self ):
		try:
			self.sesTarget.commit()
		except sqlalchemy.exc.IntegrityError as e:
			self.sesTarget.rollback()
			raise e
		
	# ##############################
	# # I STOPPED HERE.....
	# ##############################

	# 		try:
	# 			processedJobLog = jobLogProcessing.processJobLog( srcJobLog, sesTarget )
	# 		except NoResultFound as e:
	# 			logger.warning( 'Constraint Failed to match a record from {o.schema}.{o.__tablename__};  Record @ emplid: {o.emplid}, deptid: {o.deptid}, jobcode: {o.jobcode}, effdt: {o.effdt}, action: {o.action}, action_reason: {o.action_reason};'.format(o=srcJobLog) )			
	# 		except Exception as e:
	# 			logger.error( 'Code Failure:', exc_info=True )
	# 			cleanUp(None)

	# I have a list of missing id's... now what

	def getSourceCache( self ):
		return self.cacheSource

	def setOverrideSource( self, overrideValue ):
		self.overrideSource = overrideValue

	def setQryByList( self, missingIds ):
		self.queryByList = missingIds