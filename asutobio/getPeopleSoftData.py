import csv, codecs, sys
from models import *
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# Configure the logging
#  Reference: https://docs.python.org/2/howto/logging.html
# logFilePath = '/Users/primusdj/Documents/code/python/elt/asudwToBioPublic/tmp/peopleSoftLog.log'


# Build the connection to the database(s)... following worked for to the sql server:
# engine_source = create_engine('mssql+pymssql://app_sqlAlchemy:forthegipperNotReagan4show@biomssql.biodesign.asu.edu:1433/PeopleSoft_HRData')
engine_source = create_engine('mssql+pymssql://app_sqlAlchemy:forthegipperNotReagan4show@biosql.biodesign.asu.edu:1433/PeopleSoft_HRData')
# engine_target = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public_ps')
engine_target = create_engine('mysql+mysqldb://app_sqlalchemy:forthegipperNotReagan4show@dbdev.biodesign.asu.edu/bio_public_ps', connect_args =  {'local_infile':1})


# Clean Up The Data Before migration
PsPublic.metadata.drop_all(engine_target)

#
# Create the BioPublic models, create the tables in the target database we will be working with.
#

PsPublic.metadata.create_all(engine_target)

# FILE Processor...
# LOAD The Fresh Data into bio_public_ps....
for table_name in PsPublic.metadata.tables.keys():
  # table_name = str.upper(table_name)
  print 'Start Processing:\t{!s}'.format(table_name)
  # columns_list = [ table_name +'.'+ str.upper(column) for column in PsPublic.metadata.tables[table_name].columns.keys() if column !='id' ]
  columns_list = [ table_name +'.'+ column for column in PsPublic.metadata.tables[table_name].columns.keys() if column not in ['id','source_hash'] ]
  stm_columns = ', '.join(map(str,columns_list))
  # what object are we working with?  The sourceCondition method returns the filter we will apply against the asu system.
  obj = getClassByTablename(table_name)()
  stm_where = obj.sourceCondition()

  # Where only needs to be applied when we select from peopleSoft.
  query_source = "SELECT {!s} FROM {!s} WHERE {!s}".format(stm_columns, table_name, stm_where)
  # query_source = "SELECT {!s} FROM {!s}".format(stm_columns, table_name)

  # pull the data out of the source database and write to the csv file...
  csvFilePath = '/Users/primusdj/Documents/code/python/elt/asudwToBioPublic/tmp/{!s}.csv'.format(table_name)

  try:
    ps_dump = csv.writer(codecs.open(csvFilePath,'w+','utf-8'))
    for row in engine_source.execute(query_source):
      ps_dump.writerow(row)

  except:
    e_list = [ e for e in sys.exc_info() ]
    for e_item in e_list:
      print e_item

  # try:
  # loading the csv file into the database.
  # query_target = "LOAD DATA LOCAL INFILE '{!s}' INTO TABLE {!s} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ({!s})".format(csvFilePath, table_name, stm_columns)
  query_target = "LOAD DATA LOCAL INFILE '{!s}' INTO TABLE {!s} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ({!s})".format(csvFilePath, table_name, stm_columns,)
  engine_target.execute(query_target)

  print '\tUPDATE source_hash value...'
  query_update_hash = "\
    UPDATE {!s} AS here\
      INNER JOIN (\
        SELECT {!s}.id, SHA2(CONCAT_WS({!s}),256) as source_hash\
        FROM {!s} AS {!s}\
      ) AS sub_query ON (here.id = sub_query.id)\
    SET here.source_hash = sub_query.source_hash\
    WHERE here.id = sub_query.id\
  ".format(table_name, table_name, stm_columns, table_name, table_name)

  engine_target.execute(query_update_hash)

  print '\tUPDATE dates with 0000-00-00 values...'
  for column_date in obj.date.property.columns


  # except StatementError as e:
  #   "SQL LOAD error({0}): {1}".format(e.statement)

  print 'Ended\n'
