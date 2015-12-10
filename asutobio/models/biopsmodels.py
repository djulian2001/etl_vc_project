import os

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base

# each table from people soft will need a new primary key called id...
# perhaps other common elements will be required.
class CommonBase(object):

    id = Column( Integer, primary_key=True )
    """The id: because it just makes it easier to have one, but mostly ignored"""

    source_hash = Column( String(64), nullable=True )
    """The source_hash: A hash of the source record which will indicate if a record needs to be updated"""

    
BioPs = declarative_base( cls=CommonBase )

# DONE
class BioPsPeople( BioPs ):
    __tablename__ = 'people'
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
    email_address = Column( String(95) )
    eid = Column( String(384) )
    birthdate = Column( Date )
    last_update = Column( DateTime(), nullable=False )

# DONE
class BioPsPersonExternalLinks( BioPs ):
    __tablename__ = 'person_externallinks'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    facebook = Column(  String(255), nullable=True )
    twitter = Column(  String(255), nullable=True )
    google_plus = Column(  String(255), nullable=True )
    linkedin = Column(  String(1024), nullable=True )

# DONE
class BioPsPersonWebProfile( BioPs ):
    __tablename__ = 'person_webprofile'
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

# done
class BioPsJobs( BioPs ):
    __tablename__ = 'person_jobs'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    empl_rcd = Column( Numeric(asdecimal=False), nullable=False )
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
    last_update = Column( DateTime(), nullable=False )
    department_directory = Column( String(255) )

# done
class BioPsPhones( BioPs ):
    __tablename__ = 'person_phones'
    # below are the data fields out of peopleSoft
    emplid = Column( Integer, nullable=False )
    phone_type = Column( String(7), nullable=False )
    phone = Column( String(31), nullable=False )
    last_update = Column( DateTime(), nullable=False )

# done
class BioPsAddresses( BioPs ):
    __tablename__ = "person_addresses"
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


class BioPsSubAffiliations( BioPs ):
    __tablename__ = 'person_subaffiliations'
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

class BiodesignSubAffiliations( BioPs ):
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

class BioPsDepartments( BioPs ):
    __tablename__ = 'departments'
    # below are the data fields out of peopleSoft
    deptid = Column( String(15), nullable = False )
    effdt = Column( DATE(), nullable = False )
    eff_status = Column( String(7), nullable = False )
    descr = Column( String(31), nullable = False )
    descrshort = Column( String(15), nullable = False )
    location = Column( String(15), nullable = False )
    budget_deptid = Column( String(15), nullable = False )


# class BioPsJobHistory( BioPs ):
#     __tablename__ = 'person_jobshistory'
#     # below are the data fields out of peopleSoft
#     emplid = Column( Integer, nullable=False )
#     effdt = Column( Date, nullable=False )
#     deptid = Column( String(15), nullable=False )
#     jobcode = Column( String(7), nullable=False )
#     action = Column( String(7), nullable=False )
#     action_reason = Column( String(7), nullable=False )
#     job_entry_dt = Column( Date, nullable=True )
#     dept_entry_dt = Column( Date, nullable=True )
#     hire_dt = Column( Date, nullable=True )
#     last_hire_dt = Column( Date, nullable=True )
#     termination_dt = Column( Date, nullable=True )
#     asgn_start_dt = Column( Date, nullable=True )
#     lst_asgn_start_dt = Column( Date, nullable=True )
#     asgn_end_dt = Column( Date, nullable=True )

#     def sourceCondition(self):
#         condition = "\
#             {!s}.emplid IN\
#             (\
#                 SELECT JPS.EMPLID FROM SYSADM{!s}PS_JOB JPS WHERE JPS.DEPTID IN \
#                 (\
#                     SELECT J.DEPTID FROM DIRECTORY{!s}JOB J WHERE J.DEPARTMENT LIKE '%Biodesign%' GROUP BY J.DEPARTMENT, J.DEPTID\
#                 )\
#             )".format(self.__tablename__,'_','_')
#         return condition


# ###############################################################################
# #  BELOW are the FAR Evaluation our source of citation and research street cred
# ###############################################################################

class BioPsFarEvaluations( BioPs ):
    __tablename__ = 'far_evaluations'
    
    # below are the data fields out of peopleSoft
    evaluationid = Column( Integer, nullable=False )
    src_sys_id = Column( String(5), nullable=False )
    calendaryear = Column( Integer, nullable=False )
    emplid = Column( Integer, nullable=False )
    asuriteid = Column( String(23), nullable=True )
    asuid = Column( String(7), nullable=True )
    faculty_rank_title = Column( String(7), nullable=True )
    job_title = Column( String(50), nullable=True )
    tenure_status_code = Column( String(7), nullable=True )
    tenurehomedeptcode = Column( String(7), nullable=True )
    extensiondate = Column( DateTime(), nullable=True )
    completed = Column( String(7), nullable=True )
    dtcreated = Column( DateTime(), nullable=True )
    dtupdated = Column( DateTime(), nullable=True )
    userlastmodified = Column( String(7), nullable=True )
    load_error = Column( String(7), nullable=True )
    data_origin = Column( String(7), nullable=True )
    created_ew_dttm = Column( DateTime(), nullable=True )
    lastupd_dw_dttm = Column( DateTime(), nullable=True )
    batch_sid = Column( Integer, nullable=False )


class BioPsFarConferenceProceedings( BioPs ):
    __tablename__ = 'far_conferenceproceedings'
    
    conferenceproceedingid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( Text(), nullable = False )
    title = Column( Text(), nullable = True )
    journalname = Column( Text(), nullable = True )
    refereed = Column( String(1), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String(103), nullable = True )
    pages = Column( String(207), nullable = True )
    webaddress = Column( String(503), nullable = True )
    abstract = Column( Text(), nullable = True )
    additionalinfo = Column( Text(), nullable = True )
    dtcreated = Column( Date(), nullable = False )
    dtupdated = Column( Date(), nullable = True )
    userlastmodified = Column( String(23), nullable = False )
    ispublic = Column( String(1), nullable = False )
    activityid = Column( Integer, nullable = True )
    load_error = Column( String(1), nullable = False )
    data_origin = Column( String(1), nullable = False )
    created_ew_dttm = Column( Date(), nullable = True )
    lastupd_dw_dttm = Column( Date(), nullable = True )
    batch_sid = Column( Integer, nullable = False )


class BioPsFarAuthoredBooks( BioPs ):
    __tablename__ = 'far_authoredbooks'
    
    authoredbookid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( Text(), nullable = False )
    title = Column( Text(), nullable = False )
    publisher = Column( String(250), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    pages = Column( String(103), nullable = True )
    isbn = Column( String(103), nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String(103), nullable = True )
    edition = Column( String(103), nullable = True )
    publicationcity = Column( String(103), nullable = True )
    webaddress = Column( String(503), nullable = True )
    translated = Column( String(1), nullable = True )
    additionalinfo = Column( Text(), nullable = True )
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


class BioPsFarRefereedarticles( BioPs ):
    __tablename__ = 'far_refereedarticles'
    
    refereedarticleid = Column( Integer, nullable = False)
    src_sys_id = Column( String( 5 ), nullable = False)
    evaluationid = Column( Integer, nullable = False)
    authors = Column( Text(), nullable = False)
    title = Column( Text(), nullable = True)
    journalname = Column( Text(), nullable = True)
    publicationstatuscode = Column( Integer, nullable = True)
    publicationyear = Column( Integer, nullable = True)
    volumenumber = Column( String( 103 ), nullable = True)
    pages = Column( String( 103 ), nullable = True)
    webaddress = Column( String( 503 ), nullable = True)
    translated = Column( String(1), nullable = True)
    abstract = Column( Text(), nullable = True)
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


class BioPsFarNonrefereedarticles( BioPs ):
    __tablename__ = 'far_nonrefereedarticles'
    
    nonrefereedarticleid = Column( Integer, nullable = False )
    src_sys_id = Column( String( 5 ), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( Text(), nullable = False )
    title = Column( Text(), nullable = False )
    journalname = Column( Text(), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String( 103 ), nullable = True )
    pages = Column( String( 103 ), nullable = True )
    webaddress = Column( Text(), nullable = True )
    translated = Column( String(1), nullable = True )
    abstract = Column( Text(), nullable = True )
    additionalinfo = Column( Text(), nullable = True )
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


class BioPsFarEditedbooks( BioPs ):
    __tablename__ = 'far_editedbooks'
    
    editedbookid = Column( Integer, nullable=False)
    src_sys_id = Column( String( 5 ), nullable=False)
    evaluationid = Column( Integer, nullable=False)
    authors = Column( Text(), nullable=True)
    title = Column( Text(), nullable=False)
    publisher = Column( String( 255 ), nullable=True)
    publicationstatuscode = Column( Integer, nullable=True)
    pages = Column( String( 103 ), nullable=True)
    isbn = Column( String( 103 ), nullable=True)
    publicationyear = Column( Integer, nullable=True)
    volumenumber = Column( String( 103 ), nullable=True)
    edition = Column( String( 103 ), nullable=True)
    publicationcity = Column( String( 103 ), nullable=True)
    webaddress = Column( Text(), nullable=True)
    translated = Column( String( 1 ), nullable=True)
    additionalinfo = Column( Text(), nullable=True)
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


class BioPsFarBookChapters( BioPs ):
    __tablename__ = 'far_bookchapters'
    
    bookchapterid = Column( Integer, nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer, nullable=False )
    bookauthors = Column( Text(), nullable=True )
    booktitle = Column( Text(), nullable=True )
    chapterauthors = Column( Text(), nullable=True )
    chaptertitle = Column( Text(), nullable=True )
    publisher = Column( String( 255 ), nullable=True )
    publicationstatuscode = Column( Integer, nullable=True )
    pages = Column( String( 103 ), nullable=True )
    isbn = Column( String( 103 ), nullable=True )
    publicationyear = Column( Integer, nullable=True )
    volumenumber = Column( String( 55 ), nullable=True )
    edition = Column( String( 55 ), nullable=True )
    publicationcity = Column( Text(), nullable=True )
    webaddress = Column( Text(), nullable=True )
    translated = Column( String( 1 ), nullable=True )
    additionalinfo = Column( Text(), nullable=True )
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


class BioPsFarBookReviews( BioPs ):
    __tablename__ = 'far_bookreviews'
    
    bookreviewid = Column( Integer , nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer , nullable=False )
    bookauthors = Column( Text(), nullable=True )
    booktitle = Column( Text(), nullable=True )
    journalname = Column( Text(), nullable=True )
    publicationstatuscode = Column( Integer , nullable=True )
    journalpages = Column( String( 103 ), nullable=True )
    journalpublicationyear = Column( Integer , nullable=True )
    journalvolumenumber = Column( String( 103 ), nullable=True )
    webaddress = Column( Text(), nullable=True )
    additionalinfo = Column( Text(), nullable=True )
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


class BioPsFarEncyclopediaarticles( BioPs ):
    __tablename__ = 'far_encyclopediaarticles'
    
    encyclopediaarticleid = Column( Integer , nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer , nullable=False )
    authors = Column( Text(), nullable=False )
    title = Column( Text(), nullable=True )
    publicationname = Column( Text(), nullable=True )
    publicationstatuscode = Column( Integer , nullable=True )
    pages = Column( String( 103 ), nullable=True )
    publicationyear = Column( Integer , nullable=True )
    publisher = Column( String( 103 ), nullable=True )
    webaddress = Column( Text(), nullable=True )
    additionalinfo = Column( Text(), nullable=True )
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


class BioPsFarShortstories( BioPs ):
    __tablename__ = 'far_shortstories'
    
    shortstoryid = Column( Integer , nullable=False )
    src_sys_id = Column( String( 5 ), nullable=False )
    evaluationid = Column( Integer , nullable=False )
    authors = Column( Text(), nullable=False )
    title = Column( Text(), nullable=True )
    publicationname = Column( Text(), nullable=True )
    publicationstatuscode = Column( Integer , nullable=True )
    pages = Column( String( 103 ), nullable=True )
    publicationyear = Column( Integer , nullable=True )
    publisher = Column( String( 103 ), nullable=True )
    webaddress = Column( Text(), nullable=True )
    translated = Column( String( 1 ), nullable=True )
    additionalinfo = Column( Text(), nullable=True )
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

# ---- far_editedbooks
# ---- far_bookchapters
# ---- far_bookreviews
# ---- far_shortstories ( no data )
# ---- far_evaluations
# ---- far_encyclopediaarticles ( no data )
# ---- far_nonrefereedarticles
# ---- far_refereedarticles  ()
