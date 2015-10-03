from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


engine = create_engine('sqlite:////tmp/test.db', convert_unicode=True)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

# 	This flask example might not work with my models because the models are already declarative_base
# 	by already using this, is there a scoping issue?
#	I also have 3 connections to maintain:
#		1) to peoplesoft using the oracle api (ps)
#		2) to a mysql with file load option enabled (copy)
#		3) to a mysql (bio)
Base = declarative_base()
Base.query = db_session.query_property()

# def init_src_db():
def init_ps_db():
    """
    	document this function
    """
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    import .psETL.models  #
    # Base
    Base.metadata.create_all(bind=engine)

def init_copy_db():
    """
    	document this function
    """
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    import .psETL.models  #
    Base.metadata.create_all(bind=engine)

def init_bio_db():
    """
    	document this function
    """
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    import .bioETL.models  #
    Base.metadata.create_all(bind=engine)
