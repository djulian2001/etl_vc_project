from sqlalchemy.sql import text
from sqlalchemy import func
import hashlib

from models.asudwpsmodels import AsuDwPsDepartments, AsuDwPsPerson, AsuDwPsSubAffiliations, AsuDwPsJobsLog
from models.biopublicmodels import SubAffiliations

###############################################################################
# General Appliclation Utilitie Functions:
###############################################################################

def resetUpdatedFlag( sesTarget, tblName ):
	"""
		Each run of the script requires that the updated_flag field be set to False
		Run a statement at the sql level against the session passed in.
		@Return: no return is required, the session will manage the transaction at
			the database level.
	"""
	sesTarget.execute( text( "UPDATE %s SET updated_flag = :resetFlag" % ( tblName ) ), { "resetFlag" : 0 } )


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


################################################################################
# Biodesign Filters:
#   Using the ORM expression language of sqlalchemy to extract the various sub
#   querries, or query patterns used frequently to extract the data from people
#   soft
################################################################################

class BiodesignSubAffiliationCodes( object ):
    """The class that gets and returns the biodesign subaffiliation codes."""
    def __init__( self, sesTarget ):
        self.sesTarget = sesTarget
        self._subAffCodes = None
        self.setSubAffCodes()

    @property
    def subAffCodes( self ):
        try:
            assert self._subAffCodes is not None, "Applications state dependency not met, set the list for subaffiliations codes"
            return self._subAffCodes
        except AssertionError as e:
            raise e

    def setSubAffCodes( self ):
        if not self._subAffCodes:
            biodesignSubAffObjs = self.sesTarget.query( SubAffiliations.code ).filter( SubAffiliations.deleted_at == None ).all()
            if biodesignSubAffObjs:
                self._subAffCodes = [ bdiSubAffObj.code for bdiSubAffObj in biodesignSubAffObjs ]
            else:
                self._subAffCodes = None



class AsuPsBioFilters():
    """
    Biodesign Filters:
        Using the ORM expression language of sqlalchemy to extract the various sub
    querries, or query patterns used frequently to extract the data from people
    soft
        There will be required filters that will be required and need to be pre-defined.
    the primary example of this is the list of biodesign people that are a subset of
    all the records in people soft.
    
    Takes a sqlalchemy session to initializes the the class...

    """
    def __init__(self, session, subAffsCodes ):
        self.session = session
        self.subAffsCodes = subAffsCodes

    def getBiodesignDeptids( self, subQuery=True ):
        """
            The list of department ids that have the matching condition of 'E08%' in the
            deptid.

        """
        # biodesignDeptids = []
        sub_groups = (
            self.session.query(
                AsuDwPsDepartments.deptid ).filter(
                    AsuDwPsDepartments.deptid.like( "E08%" )
                ).group_by( AsuDwPsDepartments.deptid )
            )
        
        if subQuery == True:
            return sub_groups.subquery()
        else:
            return sub_groups


    def getAllBiodesignEmplidList(self, subQuery=True ):
        """ 
        The primary list of people that will be used to define the list of emplid we
        are intrested in from peoplesoft.
        
        The subaffiliation codes (subAffsCodes) were provided by the HR / Operations team.
        These used to be manually assigned, but now they are pulled in from the target
            database, to configure the subqry's where in's.  This allows for additions to
            the complex relationships that exist between people and departments.

        Each Job record in PS_JOB has an action code associated with them.  The ones
        provided in the jobActionExcludeCodes indicate the removal of a person from 
        the current ASU log, making them, inactive and not a record we wish to get.
        """
        
        jobActionExcludeCodes = ['TER','RET']
        
        sub_groups = self.getBiodesignDeptids()

        affiliation_emplid_list = (
            self.session.query(
                AsuDwPsSubAffiliations.emplid.label( "emplid" ) ).join(
                    AsuDwPsPerson, AsuDwPsPerson.emplid == AsuDwPsSubAffiliations.emplid
                ).filter( AsuDwPsPerson.asurite_id.isnot( None ) ).join(
                    sub_groups, AsuDwPsSubAffiliations.deptid == sub_groups.c.deptid
                ).filter(
                    AsuDwPsSubAffiliations.subaffiliation_code.in_(
                        self.subAffsCodes
                    )
                )
            )

        sub_jobs = (
            self.session.query(
                AsuDwPsJobsLog.emplid, 
                AsuDwPsJobsLog.deptid, 
                AsuDwPsJobsLog.effdt, 
                AsuDwPsJobsLog.action,
                func.row_number().over(
                    partition_by = [AsuDwPsJobsLog.emplid, AsuDwPsJobsLog.main_appt_num_jpn],
                    order_by = AsuDwPsJobsLog.effdt.desc()
                    ).label( 'rn' )
                )
            ).subquery()

        employee_emplid_list = (
            self.session.query( sub_jobs.c.emplid.label( "emplid" ) ).join(
                sub_groups, sub_jobs.c.deptid == sub_groups.c.deptid ).filter(
                    sub_jobs.c.rn == 1 ).filter(
                        ~sub_jobs.c.action.in_( jobActionExcludeCodes ) )
            )

        emplid_list = ( employee_emplid_list.union( affiliation_emplid_list ) )

        if subQuery == True:
            return emplid_list.subquery()
        else:
            return emplid_list

