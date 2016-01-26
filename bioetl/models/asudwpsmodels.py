from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base

################################################################################
# Biodesign Filters:
#   Using the ORM expression language of sqlalchemy to extract the various sub
#   querries, or query patterns used frequently to extract the data from people
#   soft
################################################################################


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
    def __init__(self, session):
        self.session = session

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
        
        The subaffiliation codes (subAffsCodes) were provided by the HR / Operations
        team, these codes are manually assigned within people soft to capture the more
        complex people relationships that can exist between Biodesign departments.

        Each Job record in PS_JOB has an action code associated with them.  The ones
        provided in the jobActionExcludeCodes indicate the removal of a person from 
        the current ASU log, making them, inactive and not a record we wish to get.
        """
        # The following are the "where in" portion / filters to use, extend this list as needed...
        # sub affiliation codes provided by the 
        subAffsCodes = ['BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL']
        jobActionExcludeCodes = ['TER','RET']
        
        sub_groups = self.getBiodesignDeptids()

        affiliation_emplid_list = (
            self.session.query(
                AsuDwPsSubAffiliations.emplid.label("emplid")).join(
                    AsuDwPsPerson, AsuDwPsPerson.emplid==AsuDwPsSubAffiliations.emplid
                ).filter(AsuDwPsPerson.asurite_id.isnot(None)).join(
                    sub_groups, AsuDwPsSubAffiliations.deptid==sub_groups.c.deptid
                ).filter(
                    AsuDwPsSubAffiliations.subaffiliation_code.in_(
                        subAffsCodes
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
                    partition_by=[AsuDwPsJobsLog.emplid, AsuDwPsJobsLog.main_appt_num_jpn],
                    order_by=AsuDwPsJobsLog.effdt.desc()
                    ).label('rn')
                )
            ).subquery()

        employee_emplid_list = (
            self.session.query(sub_jobs.c.emplid.label("emplid")).join(
                sub_groups, sub_jobs.c.deptid==sub_groups.c.deptid).filter(
                    sub_jobs.c.rn == 1).filter(
                        ~sub_jobs.c.action.in_(jobActionExcludeCodes))
            )

        emplid_list = ( employee_emplid_list.union(affiliation_emplid_list) )

        if subQuery == True:
            return emplid_list.subquery()
        else:
            return emplid_list


################################################################################
# People Soft Models:
################################################################################

AsuDwPs = declarative_base()

class AsuDwPsPerson( AsuDwPs ):
    __tablename__ = 'PERSON'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    asurite_id = Column( String(23), nullable=True )
    asu_id = Column( String(23), nullable=False )
    ferpa = Column( String(7), nullable=False )
    last_name = Column( String(31), nullable=False )
    first_name = Column( String(31), nullable=False )
    middle_name = Column( String(31), nullable=False )
    display_name = Column( String(255), nullable=False )
    preferred_first_name = Column( String(255))
    affiliations = Column( String(384), nullable=False )
    email_address = Column( String(95))
    eid = Column( String(384))
    birthdate = Column( Date)
    last_update = Column( DateTime(), nullable=False )
    facebook = Column( String(255), nullable=True )
    twitter = Column( String(255), nullable=True )
    google_plus = Column( String(255), nullable=True )
    linkedin = Column( String(1024), nullable=True )
    bio = Column( Text(), nullable=True )
    research_interests = Column( Text(), nullable=True )
    cv = Column( String(1024), nullable=True )
    website = Column( String(1024), nullable=True )
    teaching_website = Column( String(1024), nullable=True )
    grad_faculties = Column( Text(), nullable=True )
    professional_associations = Column( Text(), nullable=True )
    work_history = Column( Text(), nullable=True )
    education = Column( Text(), nullable=True )
    research_group = Column( Text(), nullable=True )
    research_website = Column( String(1024), nullable=True )
    honors_awards = Column( Text(), nullable=True )
    editorships = Column( Text(), nullable=True )
    presentations = Column( Text(), nullable=True )

    __mapper_args__ = {"primary_key":[emplid]}
    __table_args__ = { "schema": schema }


class AsuDwPsPhones( AsuDwPs ):
    __tablename__ = 'PHONE'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    phone_type = Column( String(7), nullable=False )
    phone = Column( String(31), nullable=False )
    last_update = Column( DateTime(), nullable=False )

    __mapper_args__ = {"primary_key":[emplid,phone_type,phone]}
    __table_args__ = { "schema": schema }


class AsuDwPsAddresses( AsuDwPs ):
    __tablename__ = "ADDRESS"
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    address_type = Column( String(7), nullable=False )
    address1 = Column( String(63), nullable=False )
    address2 = Column( String(63), nullable=False )
    address3 = Column( String(63), nullable=False )
    address4 = Column( String(63), nullable=False )
    city = Column( String(31), nullable=False )
    state = Column( String(7), nullable=False )
    postal = Column( String(15), nullable=False )
    country_code = Column( String(7), nullable=False )
    country_descr = Column( String(31), nullable=False )
    last_update = Column( DateTime(), nullable=False )

    __mapper_args__ = {"primary_key":[emplid,address_type]}
    __table_args__ = { "schema": schema }

class AsuDwPsJobs( AsuDwPs ):
    __tablename__ = 'JOB'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    empl_rcd = Column(Numeric(asdecimal=False ), nullable=False )
    title = Column( String(255))
    department = Column( String(31))
    mailcode = Column( Integer)
    empl_class = Column( String(31))
    job_indicator = Column( String(7), nullable=False )
    location = Column( String(15), nullable=False )
    hr_status = Column( String(7), nullable=False )
    deptid = Column( String(15), nullable=False )
    empl_status = Column( String(7), nullable=False )
    fte = Column(Numeric(7,6), nullable=False )
    last_update = Column( DateTime(), nullable=False )
    department_directory = Column( String(255))

    __mapper_args__ = {"primary_key":[emplid,title,deptid]}
    __table_args__ = { "schema": schema }


class AsuDwPsSubAffiliations( AsuDwPs ):
    __tablename__ = 'SUBAFFILIATION'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    deptid = Column( String(15), nullable=False )
    subaffiliation_code = Column( String(7), nullable=False )
    campus = Column( String(7), nullable=False )
    title = Column( String(39), nullable=False )
    short_description = Column( String(23), nullable=False )
    description = Column( String(47), nullable=False )
    directory_publish = Column( String(7), nullable=False )
    department = Column( String(31), nullable=False )
    last_update = Column( DateTime(), nullable=False )
    department_directory = Column( String(255), nullable=True )

    __mapper_args__ = {"primary_key":[emplid,deptid,subaffiliation_code]}
    __table_args__ = { "schema": schema }


class AsuDwPsJobsLog( AsuDwPs ):
    __tablename__ = 'PS_JOB'
    schema = 'SYSADM'

    emplid = Column( Integer, nullable=False )
    deptid = Column( String(15), nullable = False )
    jobcode = Column( String(7), nullable = False )
    supervisor_id = Column( String(15), nullable = False )
    reports_to = Column( String(15), nullable = False )
    main_appt_num_jpn = Column(Numeric(scale=0, asdecimal=False ), nullable = False )
    effdt = Column( Date(), nullable = False )
    action = Column( String(7), nullable = False )
    action_reason = Column( String(7), nullable = False )
    action_dt = Column( Date(), nullable = True )
    job_entry_dt = Column( Date(), nullable = True )
    dept_entry_dt = Column( Date(), nullable = True )
    position_entry_dt = Column( Date(), nullable = True )
    hire_dt = Column( Date(), nullable = True )
    last_hire_dt = Column( Date(), nullable = True )
    termination_dt = Column( Date(), nullable = True )

    __mapper_args__ = { "primary_key":[emplid,deptid,jobcode,effdt,action,action_reason] }
    __table_args__ = { "schema": schema }

class AsuDwPsJobCodes( AsuDwPs ):
    __tablename__ = 'PS_JOBCODE_TBL'
    schema = 'SYSADM'

    setid = Column( String(7), nullable = False)
    jobcode = Column( String(7), nullable = False)
    effdt = Column( DATE(), nullable = False)
    src_sys_id = Column( String(7), nullable = False)
    eff_status = Column( String(1), nullable = False)
    descr = Column( String(31), nullable = False)
    descrshort = Column( String(15), nullable = False)
    setid_salary = Column( String(7), nullable = False)
    sal_admin_plan = Column( String(7), nullable = False)
    grade = Column( String(7), nullable = False)
    manager_level = Column( String(7), nullable = False)
    job_family = Column( String(7), nullable = False)
    flsa_status = Column( String(1), nullable = False)

    __mapper_args__ = { "primary_key" : [jobcode,effdt] }
    __table_args__ = { "schema" : schema }


class AsuDwPsDepartments( AsuDwPs ):
    __tablename__ = 'PS_DEPT_TBL'
    schema = 'SYSADM'
    deptid = Column( String(15), nullable = False )
    effdt = Column(DATE(), nullable = False )
    eff_status = Column( String(7), nullable = False )
    descr = Column( String(31), nullable = False )
    descrshort = Column( String(15), nullable = False )
    location = Column( String(15), nullable = False )
    budget_deptid = Column( String(15), nullable = False )

    __mapper_args__ = { "primary_key" :[deptid,descr,effdt] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarEvaluations( AsuDwPs ):
    __tablename__ = 'FAR_EVALUATIONS'
    schema = 'ASUDW'
    # below are the data fields out of peopleSoft
    evaluationid = Column( Integer, nullable=False )
    src_sys_id = Column( String(5), nullable=False )
    calendaryear = Column( Integer, nullable=False )
    emplid = Column( 'affiliateid', Integer, nullable=False )
    asuriteid = Column( String(23), nullable=True )
    asuid = Column( String(7), nullable=True )
    faculty_rank_title = Column( String(7), nullable=True )
    job_title = Column( String(50), nullable=True )
    tenure_status_code = Column( String(7), nullable=True )
    tenurehomedeptcode = Column( String(7), nullable=True )
    extensiondate = Column( DateTime(15), nullable=True )
    completed = Column( String(7), nullable=True )
    dtcreated = Column( DateTime(15), nullable=True )
    dtupdated = Column( DateTime(15), nullable=True )
    userlastmodified = Column( String(7), nullable=True )
    load_error = Column( String(7), nullable=True )
    data_origin = Column( String(7), nullable=True )
    created_ew_dttm = Column( DateTime(), nullable=True )
    lastupd_dw_dttm = Column( DateTime(), nullable=True )
    batch_sid = Column( Integer, nullable=False )

    __mapper_args__ = { "primary_key" : [ evaluationid, emplid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarConferenceProceedings( AsuDwPs ):
    __tablename__ = 'FAR_CONFERENCEPROCEEDINGS'
    schema = 'ASUDW'
    conferenceproceedingid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( String(4000), nullable = False )
    title = Column( String(1000), nullable = True )
    journalname = Column( String(1000), nullable = True )
    refereed = Column( String(1), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String(100), nullable = True )
    pages = Column( String(200), nullable = True )
    webaddress = Column( String(500), nullable = True )
    abstract = Column( Text(), nullable = True )
    additionalinfo = Column( String(1000), nullable = True )
    dtcreated = Column( Date(), nullable = False )
    dtupdated = Column( Date(), nullable = True )
    userlastmodified = Column( String(16), nullable = False )
    ispublic = Column( String(1), nullable = False )
    activityid = Column( Integer, nullable = True )
    load_error = Column( String(1), nullable = False )
    data_origin = Column( String(1), nullable = False )
    created_ew_dttm = Column( Date(), nullable = True )
    lastupd_dw_dttm = Column( Date(), nullable = True )
    batch_sid = Column( Integer, nullable = False )

    __mapper_args__ = { "primary_key" : [ conferenceproceedingid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarAuthoredBooks( AsuDwPs ):
    __tablename__ = 'FAR_AUTHOREDBOOKS'
    schema = 'ASUDW'

    authoredbookid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( String(4000), nullable = False )
    title = Column( String(1000), nullable = False )
    publisher = Column( String(250), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    pages = Column( String(100), nullable = True )
    isbn = Column( String(100), nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String(100), nullable = True )
    edition = Column( String(100), nullable = True )
    publicationcity = Column( String(100), nullable = True )
    webaddress = Column( String(500), nullable = True )
    translated = Column( String(1), nullable = True )
    additionalinfo = Column( String(2500), nullable = True )
    dtcreated = Column( Date() , nullable = False )
    dtupdated = Column( Date() , nullable = True )
    userlastmodified = Column( String(16), nullable = False )
    ispublic = Column( String(1), nullable = False )
    activityid = Column( Integer, nullable = True )
    load_error = Column( String(1), nullable = False )
    data_origin = Column( String(1), nullable = False )
    created_ew_dttm = Column( Date() , nullable = True )
    lastupd_dw_dttm = Column( Date() , nullable = True )
    batch_sid = Column( Integer, nullable = False )

    __mapper_args__ = { "primary_key" : [ authoredbookid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarRefereedarticles( AsuDwPs ):
    __tablename__ = 'FAR_REFEREEDARTICLES'
    schema = 'ASUDW'

    refereedarticleid = Column( Integer, nullable = False)
    src_sys_id = Column( String( 5 ), nullable = False)
    evaluationid = Column( Integer, nullable = False)
    authors = Column( String( 3999 ), nullable = False)
    title = Column( String( 1007 ), nullable = True)
    journalname = Column( String( 1007 ), nullable = True)
    publicationstatuscode = Column( Integer, nullable = True)
    publicationyear = Column( Integer, nullable = True)
    volumenumber = Column( String( 103 ), nullable = True)
    pages = Column( String( 103 ), nullable = True)
    webaddress = Column( String( 503 ), nullable = True)
    translated = Column( String(1), nullable = True)
    abstract = Column( CLOB(), nullable = True)
    additionalinfo = Column( String( 1007 ), nullable = True)
    dtcreated = Column( DATE(), nullable = False)
    dtupdated = Column( DATE(), nullable = True)
    userlastmodified = Column( String( 23 ), nullable = False)
    ispublic = Column( String( 1 ), nullable = False)
    activityid = Column( Integer, nullable = True)
    load_error = Column( String( 1 ), nullable = False)
    data_origin = Column( String( 1 ), nullable = False)
    created_ew_dttm = Column( DATE(), nullable = True)
    lastupd_dw_dttm = Column( DATE(), nullable = True)
    batch_sid = Column( Integer, nullable = False)

    __mapper_args__ = { "primary_key" : [ refereedarticleid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarNonrefereedarticles( AsuDwPs ):
    __tablename__ = 'FAR_NONREFEREEDARTICLES'
    schema = 'ASUDW'

    nonrefereedarticleid = Column( Integer, nullable = False )
    src_sys_id = Column( String( 5 ), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( String( 4007 ), nullable = False )
    title = Column( String( 1007 ), nullable = False )
    journalname = Column( String( 1007 ), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String( 103 ), nullable = True )
    pages = Column( String( 103 ), nullable = True )
    webaddress = Column( String( 503 ), nullable = True )
    translated = Column( String(1), nullable = True )
    abstract = Column( CLOB(), nullable = True )
    additionalinfo = Column( String( 1007 ), nullable = True )
    dtcreated = Column( DATE(), nullable = False )
    dtupdated = Column( DATE(), nullable = True )
    userlastmodified = Column( String( 23 ), nullable = False )
    ispublic = Column( String(length=1), nullable = False )
    activityid = Column( Integer, nullable = True )
    load_error = Column( String( 1 ), nullable = False )
    data_origin = Column( String( 1 ), nullable = False )
    created_ew_dttm = Column( DATE(), nullable = True )
    lastupd_dw_dttm = Column( DATE(), nullable = True )
    batch_sid = Column( Integer, nullable = False )

    __mapper_args__ = { "primary_key" : [ nonrefereedarticleid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarEditedbooks( AsuDwPs ):
    __tablename__ = 'FAR_EDITEDBOOKS'
    schema = 'ASUDW'

    editedbookid = Column( Integer, nullable=False)
    src_sys_id = Column( String( 5 ), nullable=False)
    evaluationid = Column( Integer, nullable=False)
    authors = Column( String( 4007 ), nullable=True)
    title = Column( String( 1007 ), nullable=False)
    publisher = Column( String( 255 ), nullable=True)
    publicationstatuscode = Column( Integer, nullable=True)
    pages = Column( String( 103 ), nullable=True)
    isbn = Column( String( 103 ), nullable=True)
    publicationyear = Column( Integer, nullable=True)
    volumenumber = Column( String( 103 ), nullable=True)
    edition = Column( String( 103 ), nullable=True)
    publicationcity = Column( String( 103 ), nullable=True)
    webaddress = Column( String( 503 ), nullable=True)
    translated = Column( String( 1 ), nullable=True)
    additionalinfo = Column( String( 2503 ), nullable=True)
    dtcreated = Column( DATE(), nullable=False)
    dtupdated = Column( DATE(), nullable=True)
    userlastmodified = Column( String( 23 ), nullable=True)
    ispublic = Column( String( 1 ), nullable=False)
    activityid = Column( Integer, nullable=True)
    load_error = Column( String( 1 ), nullable=False)
    data_origin = Column( String( 1 ), nullable=False)
    created_ew_dttm = Column( DATE(), nullable=True)
    lastupd_dw_dttm = Column( DATE(), nullable=True)
    batch_sid = Column( Integer, nullable=False)

    __mapper_args__ = { "primary_key" : [ editedbookid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarBookChapters( AsuDwPs ):
    __tablename__ = 'FAR_BOOKCHAPTERS'
    schema = 'ASUDW'

    bookchapterid = Column( Integer, nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer, nullable=False )
    bookauthors = Column( String( 1007 ), nullable=True )
    booktitle = Column( String( 1007 ), nullable=True )
    chapterauthors = Column( String( 1007 ), nullable=True )
    chaptertitle = Column( String( 1007 ), nullable=True )
    publisher = Column( String( 255 ), nullable=True )
    publicationstatuscode = Column( Integer, nullable=True )
    pages = Column( String( 103 ), nullable=True )
    isbn = Column( String( 103 ), nullable=True )
    publicationyear = Column( Integer, nullable=True )
    volumenumber = Column( String( 55 ), nullable=True )
    edition = Column( String( 55 ), nullable=True )
    publicationcity = Column( String( 503 ), nullable=True )
    webaddress = Column( String( 503 ), nullable=True )
    translated = Column( String( 1 ), nullable=True )
    additionalinfo = Column( String( 2503 ), nullable=True )
    dtcreated = Column( DATE(), nullable=False )
    dtupdated = Column( DATE(), nullable=True )
    userlastmodified = Column( String( 23 ), nullable=False )
    ispublic = Column( String( 1 ), nullable=False )
    activityid = Column( Integer, nullable=True )
    load_error = Column( String( 1 ), nullable=False )
    data_origin = Column( String( 1 ), nullable=False )
    created_ew_dttm = Column( DATE(), nullable=True )
    lastupd_dw_dttm = Column( DATE(), nullable=True )
    batch_sid = Column( Integer, nullable=False )

    __mapper_args__ = { "primary_key" : [ bookchapterid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarBookReviews( AsuDwPs ):
    __tablename__ = 'FAR_BOOKREVIEWS'
    schema = 'ASUDW'

    bookreviewid = Column( Integer , nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer , nullable=False )
    bookauthors = Column( String( 4007 ), nullable=True )
    booktitle = Column( String( 1007 ), nullable=True )
    journalname = Column( String( 1007 ), nullable=True )
    publicationstatuscode = Column( Integer , nullable=True )
    journalpages = Column( String( 103 ), nullable=True )
    journalpublicationyear = Column( Integer , nullable=True )
    journalvolumenumber = Column( String( 103 ), nullable=True )
    webaddress = Column( String( 503 ), nullable=True )
    additionalinfo = Column( String( 2503 ), nullable=True )
    dtcreated = Column( DATE(), nullable=False )
    dtupdated = Column( DATE(), nullable=True )
    userlastmodified = Column( String( 23 ), nullable=False )
    ispublic = Column( CHAR( 1 ), nullable=False )
    activityid = Column( Integer , nullable=True )
    load_error = Column( String( 1 ), nullable=False )
    data_origin = Column( String( 1 ), nullable=False )
    created_ew_dttm = Column( DATE(), nullable=True )
    lastupd_dw_dttm = Column( DATE(), nullable=True )
    batch_sid = Column( Integer , nullable=False )

    __mapper_args__ = { "primary_key" : [ bookreviewid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarEncyclopediaarticles( AsuDwPs ):
    __tablename__ = 'FAR_ENCYCLOPEDIAARTICLES'
    schema = 'ASUDW'

    encyclopediaarticleid = Column( Integer , nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer , nullable=False )
    authors = Column( String( 4007 ), nullable=False )
    title = Column( String( 1007 ), nullable=True )
    publicationname = Column( String( 1007 ), nullable=True )
    publicationstatuscode = Column( Integer , nullable=True )
    pages = Column( String( 103 ), nullable=True )
    publicationyear = Column( Integer , nullable=True )
    publisher = Column( String( 103 ), nullable=True )
    webaddress = Column( String( 503 ), nullable=True )
    additionalinfo = Column( String( 1007 ), nullable=True )
    dtcreated = Column( DATE(),  nullable=False )
    dtupdated = Column( DATE(),  nullable=True )
    userlastmodified = Column( String( 23 ), nullable=False )
    ispublic = Column( String( 1 ), nullable=False )
    activityid = Column( Integer , nullable=True )
    load_error = Column( String( 1 ), nullable=False )
    data_origin = Column( String( 1 ), nullable=False )
    created_ew_dttm = Column( DATE(),  nullable=True )
    lastupd_dw_dttm = Column( DATE(),  nullable=True )
    batch_sid = Column( Integer , nullable=False )

    __mapper_args__ = { "primary_key" : [ encyclopediaarticleid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class AsuDwPsFarShortstories( AsuDwPs ):
    __tablename__ = 'FAR_SHORTSTORIES'
    schema = 'ASUDW'

    shortstoryid = Column( Integer , nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer , nullable=False )
    authors = Column( String( 4007 ), nullable=False )
    title = Column( String( 1007 ), nullable=True )
    publicationname = Column( String( 1007 ), nullable=True )
    publicationstatuscode = Column( Integer , nullable=True )
    pages = Column( String( 103 ), nullable=True )
    publicationyear = Column( Integer , nullable=True )
    publisher = Column( String( 103 ), nullable=True )
    webaddress = Column( String( 503 ), nullable=True )
    translated = Column( String( 1 ), nullable=True )
    additionalinfo = Column( String( 1007 ), nullable=True )
    dtcreated = Column( DATE(), nullable=False )
    dtupdated = Column( DATE(), nullable=True )
    userlastmodified = Column( String( 23 ), nullable=False )
    ispublic = Column( String( 1 ), nullable=False )
    activityid = Column( Integer , nullable=True )
    load_error = Column( String( 1 ), nullable=False )
    data_origin = Column( String( 1 ), nullable=False )
    created_ew_dttm = Column( DATE(), nullable=True )
    lastupd_dw_dttm = Column( DATE(), nullable=True )
    batch_sid = Column( Integer , nullable=False )

    __mapper_args__ = { "primary_key" : [ shortstoryid, evaluationid ] }
    __table_args__ = { "schema" : schema }


class BiodesignSubAffiliations():
    __tablename__ = 'subaffiliations'
    code = Column( String(7), unique = True, nullable=False )
    title = Column( String(63), nullable=False )
    description = Column( String(287), nullable=False )
    proximity_scope = Column( String(15), nullable=False )
    service_access = Column( String(127), nullable=False )
    distribution_lists = Column( String(127), nullable=False )

    @staticmethod
    def seedMe():   
        seeds = [
            {
                "code":"BDAF",
                "title": "BDI Affiliated Faculty",
                "description":"ASU Faculty that are considered part of our core workforce and/or have joint appointments but are not on a Biodesign department code (employee list).  Includes faculty who supervise Biodesign employees and/or projects (usually P.I.'s)  Must have a Ph.D.",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.Faculty, DL.ORG.BIOD.XYZ.Faculty" },
            {
                "code":"BDRP",
                "title": "BDI Research Professional",
                "description":"ASU Research Staff doing or collaborating on research at Biodesign (ex: Academic Professionals, University Staff, DACT)",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.Staff, DL.ORG.BIOD.XYZ.Staff" },
            {
                "code":"BDAS",
                "title": "BDI Affiliated Staff",
                "description":"ASU non-research Staff that are considered part of our workforce.",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.Staff, DL.ORG.BIOD.XYZ.Staff" },
            {
                "code":"BDFC",
                "title": "ASU Faculty Collaborator",
                "description":"ASU faculty member coducting collaborative research with BDI faculty;  includes tenured faculty, tenure-track faculty, research faculty, adjunct faculty,  part-time temp faculty, clinical faculty (teaching or research).  Not paid on a Biodesign's department code.",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.Faculty, DL.ORG.BIOD.XYZ.Faculty" },
            {
                "code":"BDAG",
                "title": "BDI Affiliated Graduate",
                "description":"ASU graduate student intern conducting research at Biodesign but not paid on a Biodesign department code.",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.GradStudents, DL.ORG.BIOD.XYZ.GradStudents" },
            {
                "code":"BAPD",
                "title": "BDI Affiliated Post Doctorate",
                "description":"ASU Postdoctorate conducting research at Biodesign but not paid on a Biodesign department code.",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.PostDocs, DL.ORG.BIOD.XYZ.PostDocs" },
            {
                "code":"BVIP",
                "title": "Biodesign VIP",
                "description":"ASU Adminstrative contacts such as President, Provost, Deans, Foundation, OKED, Guidance Council, and EHS.,NOT included in",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.Biodesign.VIP" },
            {
                "code":"BDAU",
                "title": "BDI Affiliated Undergraduate",
                "description":"ASU undergraduate student working within Biodesign but not paid on a Biodesign department code.",
                "proximity_scope":"Internal",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.StudentWorkers, DL.ORG.BIOD.XYZ.StudentWorkers" },
            {
                "code":"BDHV",
                "title": "Biodesign HS Volunteer",
                "description":"High School Interns not paid on a Biodesign department code.",
                "proximity_scope":"External",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.HSVol, DL.ORG.BIOD.XYZ.HSVol" },
            {
                "code":"NCON",
                "title": "Consultant",
                "description":"Outside Consultant hired on contract.  Must be approved by Director of Operations -- Consultants must meet ASU criteria to have this affiliation and are paid through purchasing (will be issued 1099 Form).",
                "proximity_scope":"External",
                "service_access":"Additional services must be requested by the department",
                "distribution_lists": "No DL - email manually" },
            {
                "code":"BDEC",
                "title": "Biodesign External Collaborator",
                "description":"Research collaborator from outside ASU.  International or domestic. Ex: Corporate affiliations, individuals, private businesses, public entities, etc. doing research for Biodesign",
                "proximity_scope":"External",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.Collaborators, DL.ORG.BIOD.XYZ.Collaborators" },
            {
                "code":"NVOL",
                "title": "Volunteers",
                "description":"Professional development and retired individuals.  Note: H4 visa holders are dependents of H1B visa holders and are not allowed to work or volunteer.",
                "proximity_scope":"External",
                "service_access":"ASURITE Domain Services, Exchange Email Account, Biodesign Server Access",
                "distribution_lists": "DL.ORG.Biodesign.All, DL.ORG.BIOD.Volunteers, DL.ORG.BIOD.XYZ.Volunteers" }, ]

        return seeds

