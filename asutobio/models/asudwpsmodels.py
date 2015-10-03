from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base

class ClassName(object):
    """docstring for ClassName"""
    def __init__(self, arg):
        super(ClassName, self).__init__()
        self.arg = arg
        


class AsuPsBioFilters():
    """
        There will be required filters that will be required and need to be pre-defined.
        the primary example of this is the list of biodesign people that are a subset of
        all the records in people soft.
    """
    def __init__(self, session):
        self.session = session

    def getBiodesignDeptids(self, subQuery=True):
        """
            The list of department ids that have the matching condition of '%Biodesign%'

        """
        sub_groups = (
            self.session.query(
                AsuDwPsDepartments.deptid).filter(
                    AsuDwPsDepartments.descr.like("%Biodesign%")
                ).group_by(AsuDwPsDepartments.deptid)
            )
        
        if subQuery == True:
            return sub_groups.subquery()
        else:
            return sub_groups


    def getBiodesignEmplidList(self, subQuery=True):
        """ 
            The primary list of people that will be used to define the list of emplid we
            are intrested in from peoplesoft
        """
        # build the select statement for the list of all the people

        sub_groups = self.getBiodesignDeptids()

        affiliation_emplid_list = (
            self.session.query(
                AsuDwPsSubAffiliations.emplid).join(
                    sub_groups, AsuDwPsSubAffiliations.deptid==sub_groups.c.deptid
                ).filter(
                    AsuDwPsSubAffiliations.subaffiliation_code.in_(
                        ['BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL']
                    )
                )
            )

        sub_jobs = (
            self.session.query(
                AsuDwPsJobLog.emplid, 
                AsuDwPsJobLog.deptid, 
                AsuDwPsJobLog.effdt, 
                AsuDwPsJobLog.action,
                func.row_number().over(
                    partition_by=[AsuDwPsJobLog.emplid, AsuDwPsJobLog.main_appt_num_jpn],
                    order_by=AsuDwPsJobLog.effdt
                    ).label('rn')
                )
            ).subquery()

        employee_emplid_list = (
            self.session.query(sub_jobs.c.emplid).join(
                sub_groups, sub_jobs.c.deptid==sub_groups.c.deptid).filter(
                    sub_jobs.c.rn == 1).filter(
                        ~sub_jobs.c.action.in_(['TER','RET']))
            )

        emplid_list = (employee_emplid_list.union(affiliation_emplid_list))

        if subQuery == True:
            return emplid_list.subquery()
        else:
            return emplid_list


AsuDwPs = declarative_base()

class AsuPsPerson(AsuDwPs):
    __tablename__ = 'PERSON'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column(Integer, nullable=False)
    asurite_id = Column(String(23))
    asu_id = Column(String(23), nullable=False)
    ferpa = Column(String(7), nullable=False)
    last_name = Column(String(31), nullable=False)
    first_name = Column(String(31), nullable=False)
    middle_name = Column(String(31), nullable=False)
    display_name = Column(String(255), nullable=False)
    preferred_first_name = Column(String(255))
    affiliations = Column(String(384), nullable=False)
    email_address = Column(String(95))
    eid = Column(String(384))
    birthdate = Column(Date)
    last_update = Column(String(31), nullable=False)
    facebook = Column( String(255), nullable=True)
    twitter = Column( String(255), nullable=True)
    google_plus = Column( String(255), nullable=True)
    linkedin = Column( String(1024), nullable=True)
    emplid = Column(Integer, nullable=False)
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


class AsuDwPsPhones(AsuDwPs):
    __tablename__ = 'PHONE'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column(Integer, nullable=False)
    phone_type = Column(String(7), nullable=False)
    phone = Column(String(31), nullable=False)
    last_update = Column(DateTime(), nullable=False)

    __mapper_args__ = {"primary_key":[emplid,phone_type,phone]}
    __table_args__ = { "schema": schema }


class AsuDwPsAddresses(AsuDwPs):
    __tablename__ = "ADDRESS"
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column(Integer, nullable=False)
    address_type = Column(String(7), nullable=False)
    address1 = Column(String(63), nullable=False)
    address2 = Column(String(63), nullable=False)
    address3 = Column(String(63), nullable=False)
    address4 = Column(String(63), nullable=False)
    city = Column(String(31), nullable=False)
    state = Column(String(7), nullable=False)
    postal = Column(String(15), nullable=False)
    country_code = Column(String(7), nullable=False)
    country_descr = Column(String(31), nullable=False)
    last_update = Column(DateTime(), nullable=False)

    __mapper_args__ = {"primary_key":[emplid,address_type]}
    __table_args__ = { "schema": schema }

class AsuDwPsJobs(AsuDwPs):
    __tablename__ = 'JOB'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column(Integer, nullable=False)
    empl_rcd = Column(Numeric(asdecimal=False), nullable=False)
    title = Column(String(255))
    department = Column(String(31))
    mailcode = Column(Integer)
    empl_class = Column(String(31))
    job_indicator = Column(String(7), nullable=False)
    location = Column(String(15), nullable=False)
    hr_status = Column(String(7), nullable=False)
    deptid = Column(String(15), nullable=False)
    empl_status = Column(String(7), nullable=False)
    fte = Column(Numeric(7,6), nullable=False)
    last_update = Column(DateTime(), nullable=False)
    department_directory = Column(String(255))

    __mapper_args__ = {"primary_key":[emplid,title,deptid]}
    __table_args__ = { "schema": schema }

class AsuDwPsSubAffiliations(AsuDwPs):
    __tablename__ = 'SUBAFFILIATION'
    schema = 'DIRECTORY'
    # below are the data fields out of peopleSoft
    emplid = Column(Integer, nullable=False)
    deptid = Column(String(15), nullable=False)
    subaffiliation_code = Column(String(7), nullable=False)
    campus = Column(String(7), nullable=False)
    title = Column(String(39), nullable=False)
    short_description = Column(String(23), nullable=False)
    description = Column(String(47), nullable=False)
    directory_publish = Column(String(7), nullable=False)
    department = Column(String(31), nullable=False)
    last_update = Column(DateTime(), nullable=False)
    department_directory = Column(String(255), nullable=True)

    __mapper_args__ = {"primary_key":[emplid,deptid,subaffiliation_code]}
    __table_args__ = { "schema": schema }


class AsuDwPsJobLog(AsuDwPs):
    __tablename__ = 'PS_JOB'
    schema = 'SYSADM'
    emplid = Column(Integer, nullable=False)
    deptid = Column(String(15), nullable = False)
    jobcode = Column(String(7), nullable = False)
    main_appt_num_jpn = Column(Numeric(scale=0, asdecimal=False), nullable = False)
    effdt = Column(Date(), nullable = False)
    action = Column(String(7), nullable = False)
    action_reason = Column(String(7), nullable = False)
    action_dt = Column(Date(), nullable = True)
    job_entry_dt = Column(Date(), nullable = True)
    dept_entry_dt = Column(Date(), nullable = True)
    position_entry_dt = Column(Date(), nullable = True)
    hire_dt = Column(Date(), nullable = True)
    last_hire_dt = Column(Date(), nullable = True)
    termination_dt = Column(Date(), nullable = True)

    __mapper_args__ = {"primary_key":[emplid,deptid,jobcode,effdt,action,action_reason]}
    __table_args__ = { "schema": schema }


class AsuDwPsDepartments(AsuDwPs):
    __tablename__ = 'PS_DEPT_TBL'
    schema = 'SYSADM'
    deptid = Column(String(15), nullable = False)
    effdt = Column(DATE(), nullable = False)
    eff_status = Column(String(7), nullable = False)
    descr = Column(String(31), nullable = False)
    descrshort = Column(String(15), nullable = False)
    location = Column(String(15), nullable = False)
    budget_deptid = Column(String(15), nullable = False)
    # descr_log = Column(String(255), nullable = False)

    __mapper_args__ = {"primary_key":[deptid,descr,effdt]}
    __table_args__ = { "schema": schema }

