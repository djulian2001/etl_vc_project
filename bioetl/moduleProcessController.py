from sqlalchemy.orm.exc import NoResultFound
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
		self.tablename = self.module.getTableName()
		self.missingIds = []

	def processSource( self ):
		"""
			Might be that there is no need for this method, just to move this process
			into the init method, but it seems wiser to trigger the events, or not we
	  	"""
		resetUpdatedFlag( self.sesTarget, self.tablename )
		self.commitThis()

		if not self.overrideSource:
			srcDataResults = self.module.getSourceData( self.sesSource, self.queryByList if self.queryByList else None ) 
		else:
			srcDataResults = self.overrideSource

		if not self.queryByList:
			self.cacheSource = srcDataResults
		
		try:
			iRecords = 1
			for srcData in srcDataResults:
				try:
					processedSrcData = self.module.processData( srcData, self.sesTarget )
				except NoResultFound as e:
					logger.warning( 'Constraint Failed to match a record from {schema}.{tblename};  Record identified as : {recId};'
									 .format(	schema=srcData.schema,
									 			tblename=srcData.__tablename__,
									 			recId=srcData.emplid if srcData.emplid else 'Not a personnel record.' ) )
					if srcData.emplid:
						"""Source of the missing ids, cache all missing values for handling later"""
						self.appendMissingEmplid( srcData.emplid )
					iRecords += 1
					break
				except Exception as e:
					logger.error( 'Code Failure:', exc_info=True )
					raise e

				if processedSrcData:
					self.sesTarget.add( processedSrcData )
					if iRecords % 1000 == 0:
						self.flushThis()
					iRecords += 1

			self.commitThis()
			resetUpdatedFlag( self.sesTarget, self.tablename )
			self.commitThis()
			logger.info("{tblname} source records processed: {actions} of {totals} records."
						.format(	tblname=self.tablename,
								 	actions=iRecords-1,
								 	totals=len( srcDataResults ) ) )
		
		except TypeError as e:
			logger.warning(" {tblname} data source returned no values in {srcData}"
							.format(	tblname=self.tablename,
										srcData="list data set {}".format( self.queryByList ) if self.queryByList else "tables data set" ),
							 )
		# print __name__, self.missingIds

	def cleanTarget( self ):
		"""Soft delete the data thats no longer found int the source db"""
		tgtDataRemovals = self.module.getTargetData( self.sesTarget )
		iRecords = 1
		for dataRemoval in tgtDataRemovals:
			processedDataRemoval = self.module.softDeleteData( dataRemoval, self.cacheSource )
			if processedDataRemoval:
				self.sesTarget.add( processedDataRemoval )
				if iRecords % 1000 == 0:
					self.flushThis()
				iRecords += 1
		self.commitThis()
		logger.info("{tblname} target records removed: {actions} of {totals} records"
					.format(	tblname=self.tablename,
								actions=iRecords-1,
								totals=len( tgtDataRemovals ) ) )


	def commitThis( self ):
		try:
			self.sesTarget.commit()
		except sqlalchemy.exc.IntegrityError as e:
			self.sesTarget.rollback()
			raise e
	
	def flushThis( self ):
		try:
			self.sesTarget.flush()
		except sqlalchemy.exc.IntegrityError as e:
			self.sesTarget.rollback()
			raise e

	def appendMissingEmplid( self, missedId ):
		"""Add an emplid to the list of missing ids found during the modules processing"""
		try:
			if type( missedId ) is int:
				self.missingIds.append( missedId )
		except TypeError:
			logger.warning( "Method appendMissingEmplid, passed wrong data type: {} is not an int.".format( missedId ) )
			pass
			
	# I have a list of missing id's... now what

	def getSourceCache( self ):
		return self.cacheSource

	def setOverrideSource( self, overrideValue ):
		self.overrideSource = overrideValue

	def setQryByList( self, runTheseIds ):
		self.queryByList = runTheseIds
