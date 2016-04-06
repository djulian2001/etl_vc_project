from moduleProcessController import ModuleProcessController

from processControllers import personProcessing
from processControllers import personWebProfileProcessing
from processControllers import personExternalLinkProcessing
from processControllers import personAddressProcessing
from processControllers import personPhoneProcessing

class BioPeopleTables( object ):
	"""The contro"""
	def __init__( self, sesSource, sesTarget, idList=[] ):
		""""The order we process the people data in..."""
		self.runIds = idList
		self.sesSource = sesSource
		self.sesTarget = sesTarget
		self.foundMissingIds = None

	def appendMissingIds( self, idList ):
		"""Takes in a list and adds it to existing list of missing ids found"""
		self.foundMissingIds = list( set( self.foundMissingIds + idList ) )

	def getMissingIds( self ):
		"""Returns all the discovered missing emplid's during the process run"""
		return self.foundMissingIds

	def runMe( self ):
		"""
			There may be a reason to trigger this event.
			NOTE:
				Asu people is split into 3 tables in biodesign db... requires
				using the srcDataset of the people object in the other objects
				externallinks and webprofile.
		"""

		people = ModuleProcessController( personProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			people.setQryByList( self.runIds )
		people.processSource()
		people.cleanTarget()
		if people.missingIds:
			self.appendMissingIds( people.missingIds )

		peopleWebProfile = ModuleProcessController( personWebProfileProcessing, self.sesTarget )
		peopleWebProfile.setOverrideSource( people.getSourceCache() )
		peopleWebProfile.processSource()
		peopleWebProfile.cleanTarget()
		if peopleWebProfile.missingIds:
			self.appendMissingIds( peopleWebProfile.missingIds )

		peopleExternalLink = ModuleProcessController( personExternalLinkProcessing, self.sesTarget )
		peopleExternalLink.setOverrideSource( people.getSourceCache() )
		peopleExternalLink.processSource()
		peopleExternalLink.cleanTarget()
		if peopleExternalLink.missingIds:
			self.appendMissingIds( peopleExternalLink.missingIds )

		peopleAddresses = ModuleProcessController( personAddressProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peopleAddresses.setQryByList( self.runIds )
		peopleAddresses.processSource()
		peopleAddresses.cleanTarget()
		if peopleAddresses.missingIds:
			self.appendMissingIds( peopleAddresses.missingIds )

		peoplePhones = ModuleProcessController( personPhoneProcessing, self.sesTarget, self.sesSource )
		if self.runIds:
			peoplePhones.setQryByList( self.runIds )
		peoplePhones.processSource()
		peoplePhones.cleanTarget()
		if peoplePhones.missingIds:
			self.appendMissingIds( peoplePhones.missingIds )


		