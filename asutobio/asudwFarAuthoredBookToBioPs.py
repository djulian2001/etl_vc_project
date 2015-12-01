from models.asudwpsmodels import AsuDwPsFarAuthoredBooks, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarAuthoredBooks

from sharedProcesses import hashThisList

# the data pull
def getSourceFarAuthoredBooksData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarAuthoredBooks model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarAuthoredBooks ).join(
			farEvals, AsuDwPsFarAuthoredBooks.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarAuthoredBooks.ispublic !='N' ).all()
		
# the data load
def processFarAuthoredBookData( farAuthoredBook ):
	"""
		Process an AsuDwPsFarAuthoredBooks object and prepare it for insert into the target BioPsFarAuthoredBooks table
		@return: the sa add object 
	"""
	
	farAuthoredBookList = [
		farAuthoredBook.authoredbookid,
		farAuthoredBook.src_sys_id,
		farAuthoredBook.evaluationid,
		farAuthoredBook.authors,
		farAuthoredBook.title,
		farAuthoredBook.publisher,
		farAuthoredBook.publicationstatuscode,
		farAuthoredBook.pages,
		farAuthoredBook.isbn,
		farAuthoredBook.publicationyear,
		farAuthoredBook.volumenumber,
		farAuthoredBook.edition,
		farAuthoredBook.publicationcity,
		farAuthoredBook.webaddress,
		farAuthoredBook.translated,
		farAuthoredBook.additionalinfo,
		farAuthoredBook.dtcreated,
		farAuthoredBook.dtupdated,
		farAuthoredBook.userlastmodified,
		farAuthoredBook.ispublic,
		farAuthoredBook.activityid,
		farAuthoredBook.load_error,
		farAuthoredBook.data_origin,
		farAuthoredBook.created_ew_dttm,
		farAuthoredBook.lastupd_dw_dttm,
		farAuthoredBook.batch_sid ]

	farAuthoredBookHash = hashThisList( farAuthoredBookList )

	tgtFarAuthoredBook = BioPsFarAuthoredBooks(
		source_hash = farAuthoredBookHash,
		authoredbookid = farAuthoredBook.authoredbookid,
		src_sys_id = farAuthoredBook.src_sys_id,
		evaluationid = farAuthoredBook.evaluationid,
		authors = farAuthoredBook.authors,
		title = farAuthoredBook.title,
		publisher = farAuthoredBook.publisher,
		publicationstatuscode = farAuthoredBook.publicationstatuscode,
		pages = farAuthoredBook.pages,
		isbn = farAuthoredBook.isbn,
		publicationyear = farAuthoredBook.publicationyear,
		volumenumber = farAuthoredBook.volumenumber,
		edition = farAuthoredBook.edition,
		publicationcity = farAuthoredBook.publicationcity,
		webaddress = farAuthoredBook.webaddress,
		translated = farAuthoredBook.translated,
		additionalinfo = farAuthoredBook.additionalinfo,
		dtcreated = farAuthoredBook.dtcreated,
		dtupdated = farAuthoredBook.dtupdated,
		userlastmodified = farAuthoredBook.userlastmodified,
		ispublic = farAuthoredBook.ispublic,
		activityid = farAuthoredBook.activityid,
		load_error = farAuthoredBook.load_error,
		data_origin = farAuthoredBook.data_origin,
		created_ew_dttm = farAuthoredBook.created_ew_dttm,
		lastupd_dw_dttm = farAuthoredBook.lastupd_dw_dttm,
		batch_sid = farAuthoredBook.batch_sid )

	return tgtFarAuthoredBook


