import datetime

from models.biopublicmodels import EtlProcessManager

class ProcessManager( object ):
	"""ProcessManager """
	def __init__( self, sesTarget ):
		self.sesTarget = sesTarget

		self.runManager = EtlProcessManager(
					source_hash="An ETL process run.",
					created_at= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ),
					started_at= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ), )

		