from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import *

class BioCommonBase(object):
    """
        Using a Decarative approach with sqlachemy, every table using the BioPublic
        base factory will leverage from these conventions
    """
    
    __table_args__ = {'mysql_engine':'InnoDB'}
    
    # new key for keeping record versions. 
    id = Column( Integer, primary_key=True )
    """ID created on the construction of the model"""
    source_hash = Column( String(64), nullable=False )
    """The source_hash: A hash of the source record which will indicate if a record needs to be updated"""
    # created_at = Column( DateTime, default=datetime.datetime.utcnow ,nullable=False )
    created_at = Column( DateTime, nullable=False )
    """Timestamp convention of laravel the frame work being used"""
    updated_at = Column( DateTime, nullable=True )
    """Timestamp convention of laravel the frame work being used"""
    deleted_at = Column( DateTime, nullable=True )
    """ Timestamp convention of laravel the front end frame work being used
        There is not always a need to add this to all of the tables.  People are softdeleted
        NOTE: This could be a class 'mixin' class TimestampThis(object): with above Column declarative
        The below table classes would have it passed in, the same as BioPublic. 
        class TableObj( TimestampThis, BioPublic ):  etc...
    """
    updated_flag = Column( Boolean(), default=False, nullable=False )
    """The data within the target table has to be managed during the processing."""

BioPublic = declarative_base( cls=BioCommonBase )

class EtlProcessManager( BioPublic ):
    """ 
        The ProcessManager keeps track of the process each run.
    """
    __tablename__ = "etl_process_manager"
    run_status = Column(String(63), default='ETL process starting',nullable=False)
    started_at = Column( DateTime, nullable=False )
    ended_at = Column( DateTime, nullable=True )
    ending_status = Column( Boolean(), default=False, nullable=False)
    emplids_not_processed = Column( Text(), default=None, nullable=True )
    

class People( BioPublic ):
    """
        The filtered down people data we want out of peopleSoft
        Commented out are the attributes removed.
    """
    __tablename__ = "people"
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    asurite_id = Column( String(23), nullable=True )
    asu_id = Column( String(23), nullable=False )
    ferpa = Column( String(7), nullable=False )
    last_name = Column( String(31), nullable=False )
    first_name = Column( String(31), nullable=False )
    middle_name = Column( String(31), nullable=False )
    display_name = Column( String(255), nullable=False )
    preferred_first_name = Column( String(255) )
    affiliations = Column( String(384), nullable=False )
    email_address = Column( String(95), nullable=True )
    eid = Column( String(384), nullable=True )
    birthdate = Column( Date, nullable=True )
    ### last_update = Column(String(31), nullable=False)

    # one to one patern:
    web_profile = relationship("PersonWebProfile", uselist=False, backref="people")
    external_links = relationship("PersonExternalLinks", uselist=False, backref="people")
    # on child_table put
    # person_id = Column( Integer, ForeignKey( 'people.id' ) )

    # one to many patern:
    addresses = relationship( "Addresses", cascade="all, delete-orphan" )
    phone_numbers = relationship( "Phones", cascade="all, delete-orphan" )
    jobs = relationship( "Jobs", cascade="all, delete-orphan", backref="people" )
    subaffiliations = relationship( "PersonSubAffiliations", cascade="all, delete-orphan", backref="people" )
    jobslog = relationship( "JobsLog", cascade="all, delete-orphan", backref="people" )


class PersonExternalLinks( BioPublic ):
    __tablename__ = 'person_externallinks'
    
    person_id = Column( Integer, ForeignKey( 'people.id' ) )
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    facebook = Column(  String(255), nullable=True )
    twitter = Column(  String(255), nullable=True )
    google_plus = Column(  String(255), nullable=True )
    linkedin = Column(  String(1024), nullable=True )

class PersonWebProfile( BioPublic ):
    __tablename__ = 'person_webprofile'

    person_id = Column( Integer, ForeignKey( 'people.id' ) )
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    bio = Column(  Text(), nullable=True  )
    research_interests = Column(  Text(), nullable=True  )
    cv = Column(  String(1024), nullable=True  )
    website = Column(  String(1024), nullable=True  )
    teaching_website = Column(  String(1024), nullable=True  )
    grad_faculties = Column(  Text(), nullable=True  )
    professional_associations = Column(  Text(), nullable=True  )
    work_history = Column(  Text(), nullable=True  )
    education = Column(  Text(), nullable=True  )
    research_group = Column(  Text(), nullable=True  )
    research_website = Column(  String(1024), nullable=True  )
    honors_awards = Column(  Text(), nullable=True  )
    editorships = Column(  Text(), nullable=True  )
    presentations = Column(  Text(), nullable=True  )

class Addresses( BioPublic ):
    """
        The filtered down people data we want out of peopleSoft
        Commented out unneeded attributes from source schema.
        Addresses has a many to one with People
    """
    __tablename__ = "person_addresses"

    # Define A relationship...    
    person_id = Column( Integer, ForeignKey( 'people.id' ) )
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

class Phones( BioPublic ):
    """
        The filtered down people data we want out of peopleSoft
        Commented out are the attributes removed.
    """
    __tablename__ = 'person_phones'

    person_id = Column( Integer, ForeignKey( 'people.id' ) )
    # updated_flag = Column( Boolean(), default=False, nullable=False )
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    phone_type = Column( String(7), nullable=False )
    phone = Column( String(31), nullable=False )
    ### last_update = Column( String(31), nullable=False )

class Jobs( BioPublic ):
    """
        Origin: Asu directory wharehouse, directory.jobs table.
        The data appears to represent the current job(s) or position here at the
        university.  The conent provides into insight as to what department over-
        sees the position.
    """
    __tablename__ = 'person_jobs'

    person_id = Column( Integer, ForeignKey( 'people.id' ) )
    department_id = Column( Integer, ForeignKey( 'departments.id' ) )
    # updated_flag = Column( Boolean(), default=False, nullable=False )
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    empl_rcd = Column( Numeric( asdecimal=False ), nullable=False )
    title = Column( String(255) )
    department = Column( String(31) )
    mailcode = Column( Integer )
    empl_class = Column( String(31) )
    job_indicator = Column( String(7), nullable=False )
    location = Column( String(15), nullable=False )
    hr_status = Column( String(7), nullable=False )
    deptid = Column( String(15), nullable=False )
    empl_status = Column( String(7), nullable=False )
    fte = Column( Numeric(7,6), nullable=False )
    department_directory = Column( String(255) )

class SubAffiliations( BioPublic ):
    """
        Origin: Biodesign Directorate
        This data defines how the BOMS are to associate the subaffiliations with in
        peopleSoft for the internal and external affiliates that are associate to 
        Biodesign departments.
    """
    __tablename__ = 'subaffiliations'

    code = Column( String(7), unique = True, nullable=False )
    title = Column( String(63), nullable=False )
    description = Column( String(287), nullable=False )
    proximity_scope = Column( String(15), nullable=False )
    service_access = Column( String(127), nullable=False )
    distribution_lists = Column( String(127), nullable=False )

    subaffiliations = relationship( "PersonSubAffiliations" )


class PersonSubAffiliations( BioPublic ):
    """
        Origin: ASU Data wharehouse, directory.subaffiliation
        The data is managed through the peopleSoft ui, by the BOMS, the coded values
        were decided on by the Biodesign Directorate and should not be changed much.
        The references will be to a table part of the Biodesign BI aspect of this
        application call subaffiliations.
    """
    __tablename__ = 'person_department_subaffiliations'

    person_id = Column( Integer, ForeignKey( 'people.id' ), nullable=False )
    department_id = Column( Integer, ForeignKey( 'departments.id' ), nullable=False )
    subaffiliation_id = Column ( Integer, ForeignKey( 'subaffiliations.id', onupdate="CASCADE", ondelete="SET NULL" ), nullable=True )
    
    # updated_flag = Column( Boolean(), default=False, nullable=False )
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    deptid = Column( String(15), nullable=False )
    subaffiliation_code = Column( String(7), nullable=False )
    campus = Column( String(7), nullable=True )
    title = Column( String(39), nullable=True )
    short_description = Column( String(23), nullable=False )
    description = Column( String(47), nullable=False )
    directory_publish = Column( String(7), nullable=False )
    department = Column( String(31), nullable=False )
    department_directory = Column( String(255), nullable=True )


class Departments( BioPublic ):
    """
        Origin: Asu directory wharehouse, SYSADM.PS_DEPT_TBL table.
        The data has to be filter with a windows function prior to moving to mysql
        because of mysql limitations.  We filter out the descr that has an 
        'Inactive' value.
        The deptid is required to capture association data between many to many
        values.
    """
    __tablename__ = 'departments'
    # below are the data fields out of peopleSoft
    deptid = Column( String(15), unique = True, nullable = False )
    effdt = Column( DATE(), nullable = False )
    eff_status = Column( String(7), nullable = False )
    descr = Column( String(31), nullable = False )
    descrshort = Column( String(15), nullable = False )
    location = Column( String(15), nullable = False )
    budget_deptid = Column( String(15), nullable = False )

    jobs = relationship( "Jobs", cascade="all, delete-orphan", backref="departments" )
    jobslog = relationship( "JobsLog", cascade="all, delete-orphan", backref="departments" )


class JobCodes( BioPublic ):
    """
        The job codes or at least a sub set of the record, we only migrate 1 record for each
        jobcode instead of the multipules that exist in SYSADM.PS_DEPT_TBL.
    """
    __tablename__ = 'jobs'

    # below are the data fields out of peopleSoft
    jobcode = Column( String(7), nullable = False)
    effdt = Column( DATE(), nullable = False)
    setid = Column( String(7), nullable = False)
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

    jobslog = relationship( "JobsLog", cascade="all, delete-orphan", backref="jobs" )

class JobsLog( BioPublic ):
    __tablename__ = 'person_jobslog'

    person_id = Column( Integer, ForeignKey( 'people.id' ) )
    job_id = Column( Integer, ForeignKey( 'jobs.id' ) )
    department_id = Column( Integer, ForeignKey( 'departments.id' ) )
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    deptid = Column( String(15), nullable = False )
    jobcode = Column( String(7), nullable = False )
    supervisor_id = Column( String(15), nullable = False)
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

