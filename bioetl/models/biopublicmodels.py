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


BioPublic = declarative_base( cls=BioCommonBase )

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
    # person_webprofile = relationship("child_table", userlist=False, backref="people")
    # on child_table put
    # person_id = Column( Integer, ForeignKey( 'people.id' ) )

    # one to many patern:
    addresses = relationship( "Addresses", cascade="all, delete-orphan" )
    phone_numbers = relationship( "Phones", cascade="all, delete-orphan" )
    jobs = relationship( "Jobs", cascade="all, delete-orphan" )


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
    # 
    updated_flag = Column( Boolean(), default=False, nullable=False )
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
    ### last_update = Column( String(31), nullable=False )

class Phones( BioPublic ):
    """
        The filtered down people data we want out of peopleSoft
        Commented out are the attributes removed.
    """
    __tablename__ = 'person_phones'

    person_id = Column( Integer, ForeignKey( 'people.id' ) )
    updated_flag = Column( Boolean(), default=False, nullable=False )
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
    ### last_update = Column( DateTime(), nullable=False )
    department_directory = Column( String(255) )

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
    deptid = Column( String(15), nullable = False )
    effdt = Column( DATE(), nullable = False )
    eff_status = Column( String(7), nullable = False )
    descr = Column( String(31), nullable = False )
    descrshort = Column( String(15), nullable = False )
    location = Column( String(15), nullable = False )
    budget_deptid = Column( String(15), nullable = False )

