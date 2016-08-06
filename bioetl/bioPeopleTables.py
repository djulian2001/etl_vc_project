from moduleProcessController import ModuleProcessController

from processControllers import personProcessing
from processControllers import personWebProfileProcessing
from processControllers import personExternalLinkProcessing
from processControllers import personAddressProcessing
from processControllers import personPhoneProcessing
from processControllers import personJobsProcessing
from processControllers import personSubAffiliationProcessing
from processControllers import personJobLogProcessing

from sharedProcesses import BiodesignSubAffiliationCodes

class BioPeopleTables( object ):
	"""
		The contract of how people data will be extracted from the source database and
		processed against the target database.  Because we are processing snapshots at
		a point in time, but we may get a new person record showing up in our data pulls
		after the people data has already been pulled, so when this happens, or other
		odd things happen, we will catch them in the foundMissingIds class attribute. 
	"""
	def __init__( self, sesSource, sesTarget, idList=[] ):
		"""
			Takes in 3 attributes, 2 required (sqlalchemy sessions), and an id list if
			passed into the function to be processed by the instantiated class.
			Access the foundMissingIds attribute via the method getUniqueFoundMissingIds.
		"""
		self.runIds = idList
		self.sesSource = sesSource
		self.sesTarget = sesTarget
		self.foundMissingIds = []
		self._appState = None

	@property
	def appState(self):
		return self._appState 
	
	def setState( self ):
		"""Not my faviorte """
		self._appState = BiodesignSubAffiliationCodes( self.sesTarget )

	def appendMissingIds( self, idList ):
		"""Takes in a list and adds it to existing list of missing ids found"""
		for anId in idList:
			self.foundMissingIds.append( anId )

	def getUniqueFoundMissingIds( self ):
		"""Returns all the discovered missing emplid's during the process run"""
		return list( map( str, set( self.foundMissingIds ) ) )

	def runMe( self ):
		"""
			The runMe function instantiates many ModuleProcessController classes, one
			for each module that is assocaited with people data.
			The modules have to be processed in order of the data dependencies.
			Ordered as:
			... lookup tables ...
			-> people data
				-> people web profile data *
				-> people web external links data *
				-> people campus address data
				-> people campus, cell phone data
				-> people current job data
				-> people sub affiliation assignments data
				-> people asu employment history data (jobs log)

			* NOTE:
				ASU's directory.people table is split into 3 tables in biodesign db...
				requires caching the people data objects for use by the other objects
				externallinks and webprofile.
		"""

		# people
		people = ModuleProcessController( personProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			people.setQryByList( self.runIds )
		else:
			people.setState( self.appState )
		people.processSource()
		if not self.runIds:
			people.cleanTarget()
		if people.missingIds:
			self.appendMissingIds( people.missingIds )

		if people.getSourceCache():
			"""
				The reason for the other objects dependency is that the data source is
				the same queried table, we cache to save time, the data warehouse is slow,
				and we already have the data.
			"""
			peopleWebProfile = ModuleProcessController( personWebProfileProcessing, self.sesTarget )
			peopleWebProfile.setOverrideSource( people.getSourceCache() )
			peopleWebProfile.processSource()
			if not self.runIds:
				peopleWebProfile.cleanTarget()
			if peopleWebProfile.missingIds:
				self.appendMissingIds( peopleWebProfile.missingIds )

			peopleExternalLink = ModuleProcessController( personExternalLinkProcessing, self.sesTarget )
			peopleExternalLink.setOverrideSource( people.getSourceCache() )
			peopleExternalLink.processSource()
			if not self.runIds:
				peopleExternalLink.cleanTarget()
			if peopleExternalLink.missingIds:
				self.appendMissingIds( peopleExternalLink.missingIds )

		# peopleAddresses
		peopleAddresses = ModuleProcessController( personAddressProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peopleAddresses.setQryByList( self.runIds )
		else:
			peopleAddresses.setState( self.appState )
		peopleAddresses.processSource()
		
		if not self.runIds:
			peopleAddresses.cleanTarget()
		if peopleAddresses.missingIds:
			self.appendMissingIds( peopleAddresses.missingIds )

		# peoplePhones
		peoplePhones = ModuleProcessController( personPhoneProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peoplePhones.setQryByList( self.runIds )
		else:
			peoplePhones.setState( self.appState )
		peoplePhones.processSource()
		if not self.runIds:
			peoplePhones.cleanTarget()
		if peoplePhones.missingIds:
			self.appendMissingIds( peoplePhones.missingIds )

		# peopleJobs
		peopleJobs = ModuleProcessController( personJobsProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peopleJobs.setQryByList( self.runIds )
		else:
			peopleJobs.setState( self.appState )
		peopleJobs.processSource()
		if not self.runIds:
			peopleJobs.cleanTarget()
		if peopleJobs.missingIds:
			self.appendMissingIds( peopleJobs.missingIds )
		
		# peopleSubAffiliations
		peopleSubAffiliations = ModuleProcessController( personSubAffiliationProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peopleSubAffiliations.setQryByList( self.runIds )
		else:
			peopleSubAffiliations.setState( self.appState )
		peopleSubAffiliations.processSource()
		if not self.runIds:
			peopleSubAffiliations.cleanTarget()
		if peopleSubAffiliations.missingIds:
			self.appendMissingIds( peopleSubAffiliations.missingIds )

		# job logs
		peopleJobLogs = ModuleProcessController( personJobLogProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peopleJobLogs.setQryByList( self.runIds )
		else:
			peopleJobLogs.setState( self.appState )
		peopleJobLogs.processSource()
		if not self.runIds:
			peopleJobLogs.cleanTarget()
		if peopleJobLogs.missingIds:
			self.appendMissingIds( peopleJobLogs.missingIds )
