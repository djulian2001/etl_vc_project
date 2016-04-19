# IF The far data has to be added back into the bio_public models

# add this to the People() object
    far_evaluations = relationship( "FarEvaluations", cascade="all, delete-orphan", backref="people" )

class FarEvaluations( BioPublic ):
    """The far data..."""
    __tablename__ = 'far_evaluations'
    
    person_id = Column( Integer, ForeignKey( 'people.id' ) )
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

    conferenceproceedings = relationship( "FarConferenceProceedings", cascade="all, delete-orphan", backref="far_evaluations" )
    authoredbooks = relationship( "FarAuthoredBooks", cascade="all, delete-orphan", backref="far_evaluations" )
    refereedarticles = relationship( "FarRefereedarticles", cascade="all, delete-orphan", backref="far_evaluations" )
    nonrefereedarticles = relationship( "FarNonrefereedarticles", cascade="all, delete-orphan", backref="far_evaluations" )
    nonrefereedarticles = relationship( "FarEditedbooks", cascade="all, delete-orphan", backref="far_evaluations" )
    nonrefereedarticles = relationship( "FarBookChapters", cascade="all, delete-orphan", backref="far_evaluations" )
    nonrefereedarticles = relationship( "FarBookReviews", cascade="all, delete-orphan", backref="far_evaluations" )
    nonrefereedarticles = relationship( "FarEncyclopediaarticles", cascade="all, delete-orphan", backref="far_evaluations" )
    nonrefereedarticles = relationship( "FarShortstories", cascade="all, delete-orphan", backref="far_evaluations" )


class FarConferenceProceedings( BioPublic ):
    """The far data..."""
    __tablename__ = 'far_conferenceproceedings'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
    conferenceproceedingid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( Text(), nullable = False )
    title = Column( Text(), nullable = True )
    journalname = Column( Text(), nullable = True )
    refereed = Column( String(1), nullable = True )
    publicationstatuscode = Column( Integer, nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String(100), nullable = True )
    pages = Column( String(200), nullable = True )
    webaddress = Column( String(500), nullable = True )
    abstract = Column( Text(), nullable = True )
    additionalinfo = Column( Text(), nullable = True )
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


class FarAuthoredBooks( BioPublic ):
    __tablename__ = 'far_authoredbooks'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
    authoredbookid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( Text(), nullable = False )
    title = Column( Text(), nullable = False )
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


class FarRefereedarticles( BioPublic ):
    __tablename__ = 'far_refereedarticles'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
    refereedarticleid = Column( Integer, nullable = False )
    src_sys_id = Column( String(5), nullable = False )
    evaluationid = Column( Integer, nullable = False )
    authors = Column( Text(), nullable = False ) 
    title = Column( Text(), nullable = True ) 
    journalname = Column( Text(), nullable = True ) 
    publicationstatuscode = Column( Integer, nullable = True )
    publicationyear = Column( Integer, nullable = True )
    volumenumber = Column( String(100), nullable = True )
    pages = Column( String(100), nullable = True )
    webaddress = Column( String(500), nullable = True )
    translated = Column( String(1), nullable = True )
    abstract = Column( Text(), nullable = True )
    additionalinfo = Column( Text(), nullable = True ) 
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


class FarNonrefereedarticles( BioPublic ):
    __tablename__ = 'far_nonrefereedarticles'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
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


class FarEditedbooks( BioPublic ):
    __tablename__ = 'far_editedbooks'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
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


class FarBookChapters( BioPublic ):
    __tablename__ = 'far_bookchapters'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
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


class FarBookReviews( BioPublic ):
    __tablename__ = 'far_bookreviews'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
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


class FarEncyclopediaarticles( BioPublic ):
    __tablename__ = 'far_encyclopediaarticles'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
    
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


class FarShortstories( BioPublic ):
    __tablename__ = 'far_shortstories'

    far_evaluation_id = Column( Integer, ForeignKey( 'far_evaluations.id' ) )
    # below are the data fields out of peopleSoft
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




