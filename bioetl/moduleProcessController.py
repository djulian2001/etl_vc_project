from sharedProcesses import resetUpdatedFlag

import logging

logger = logging.getLogger( __name__ )
###############################################################################
class ModuleProcessController( object ):
	"""
		There is a very common pattern that is used to process almost every module 
		imported to process those specific data sets.  This is a class that will
		move all of those patterns into a single point.

	"""
	def __init__( self, aModule, sesTarget, sesSource=None ):
		self.module = aModule
		self.sesTarget = sesTarget
		self.sesSource = sesSource

	def runControllerModule( self, aList=None ):
		"""
			Might be that there is no need for this method, just to move this process
			into the init method, but it seems wiser to trigger the events, or not we
	  	"""
		tablename = self.module.getTableName()

		resetUpdatedFlag( self.sesTarget, tablename )
	
		srcDataResults = self.module.getSourceData( self.sesSource )

		iRecords = { 'updates':1, 'removed':1 }
		for srcData in srcDataResults:
			processedSrcData = self.module.processData( srcData, self.sesTarget )
			if processedSrcData:
				self.sesTarget.add( processedSrcData )

				if iRecords["updates"] % 1000 == 0:
					try:
						self.sesTarget.flush()
					except sqlalchemy.exc.IntegrityError as e:
						self.sesTarget.rollback()
						raise e
				iRecords["updates"] += 1
		try:
			self.sesTarget.commit()
		except sqlalchemy.exc.IntegrityError as e:
			self.sesTarget.rollback()
			raise e

		tgtDataRemovals = self.module.getTargetData( self.sesTarget )

		for dataRemoval in tgtDataRemovals:
			processedDataRemoval = self.module.softDeleteData( dataRemoval, srcDataResults )

			if processedDataRemoval:
				self.sesTarget.add( processedDataRemoval )

				if iRecords["removed"] % 1000 == 0:
					try:
						self.sesTarget.flush()
					except sqlalchemy.exc.IntegrityError as e:
						self.sesTarget.rollback()
						raise e
				iRecords["removed"] += 1

		logger.info("{} source records processed: {} of {} records.".format( tablename, iRecords["updates"]-1, len( srcDataResults ) ) )
		logger.info("{} target records removed: {} of {} records".format( tablename, iRecords["removed"]-1, len( tgtDataRemovals ) ) )

		try:
			self.sesTarget.commit()
		except sqlalchemy.exc.IntegrityError as e:
			self.sesTarget.rollback()
			raise e

		resetUpdatedFlag( self.sesTarget, tablename )

		try:
			self.sesTarget.commit()
		except sqlalchemy.exc.IntegrityError as e:
			self.sesTarget.rollback()
			raise e


