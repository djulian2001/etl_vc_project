from moduleProcessController import ModuleProcessController

import personProcessing
import personWebProfileProcessing
import personExternalLinkProcessing

class BioPeopleTables( object ):
	"""The contro"""
	def __init__( self, sesSource, sesTarget, idList=[] ):
		""""The order we process the people data in..."""
		self.missingIds = idList
		self.sesSource = sesSource
		self.sesTarget = sesTarget

	def addMissingIds( self, idList ):
		"""Takes in a list and adds it to existing list of missing ids found"""
		self.missingIds = list( set( self.missingIds + idList ) )

	def getMissingIds( self ):
		"""Returns all the discovered missing emplid's during the process run"""
		return self.getMissingIds

	def runMe( self ):
		"""
			There may be a reason to trigger this event.
			NOTE:
				Asu people is split into 3 tables in biodesign db... requires
				using the srcDataset of the people object in the other objects
				externallinks and webprofile.
		"""

		people = ModuleProcessController( personProcessing, self.sesTarget, self.sesSource )
		if self.missingIds:
			people.setQryByList( self.missingIds )
		people.processSource()
		people.cleanTarget()

		peopleWebProfile = ModuleProcessController( personWebProfileProcessing, self.sesTarget )
		peopleWebProfile.setOverrideSource( people.getSourceCache() )
		peopleWebProfile.processSource()
		peopleWebProfile.cleanTarget()

		peopleExternalLink = ModuleProcessController( personExternalLinkProcessing, self.sesTarget )
		peopleExternalLink.setOverrideSource( people.getSourceCache() )
		peopleExternalLink.processSource()
		peopleExternalLink.cleanTarget()
