from models.asudwpsmodels import AsuDwPsFarBookChapters, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarBookChapters

from sharedProcesses import hashThisList

# the data pull
def getSourceFarBookChaptersData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarBookChapters model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarBookChapters ).join(
			farEvals, AsuDwPsFarBookChapters.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarBookChapters.ispublic !='N' ).all()
	
# the data load
def processFarBookChapterData( farBookChapter ):
	"""
		Process an AsuDwPsFarBookChapters object and prepare it for insert into the target BioPsFarBookChapters table
		@return: the sa add object 
	"""
	
	farBookChapterList = [
		farBookChapter.bookchapterid,
		farBookChapter.src_sys_id,
		farBookChapter.evaluationid,
		farBookChapter.bookauthors,
		farBookChapter.booktitle,
		farBookChapter.chapterauthors,
		farBookChapter.chaptertitle,
		farBookChapter.publisher,
		farBookChapter.publicationstatuscode,
		farBookChapter.pages,
		farBookChapter.isbn,
		farBookChapter.publicationyear,
		farBookChapter.volumenumber,
		farBookChapter.edition,
		farBookChapter.publicationcity,
		farBookChapter.webaddress,
		farBookChapter.translated,
		farBookChapter.additionalinfo,
		farBookChapter.dtcreated,
		farBookChapter.dtupdated,
		farBookChapter.userlastmodified,
		farBookChapter.ispublic,
		farBookChapter.activityid,
		farBookChapter.load_error,
		farBookChapter.data_origin,
		farBookChapter.created_ew_dttm,
		farBookChapter.lastupd_dw_dttm,
		farBookChapter.batch_sid ]

	farBookChapterHash = hashThisList( farBookChapterList )

	tgtFarBookChapter = BioPsFarBookChapters(
		source_hash = farBookChapterHash,
		bookchapterid = farBookChapter.bookchapterid,
		src_sys_id = farBookChapter.src_sys_id,
		evaluationid = farBookChapter.evaluationid,
		bookauthors = farBookChapter.bookauthors,
		booktitle = farBookChapter.booktitle,
		chapterauthors = farBookChapter.chapterauthors,
		chaptertitle = farBookChapter.chaptertitle,
		publisher = farBookChapter.publisher,
		publicationstatuscode = farBookChapter.publicationstatuscode,
		pages = farBookChapter.pages,
		isbn = farBookChapter.isbn,
		publicationyear = farBookChapter.publicationyear,
		volumenumber = farBookChapter.volumenumber,
		edition = farBookChapter.edition,
		publicationcity = farBookChapter.publicationcity,
		webaddress = farBookChapter.webaddress,
		translated = farBookChapter.translated,
		additionalinfo = farBookChapter.additionalinfo,
		dtcreated = farBookChapter.dtcreated,
		dtupdated = farBookChapter.dtupdated,
		userlastmodified = farBookChapter.userlastmodified,
		ispublic = farBookChapter.ispublic,
		activityid = farBookChapter.activityid,
		load_error = farBookChapter.load_error,
		data_origin = farBookChapter.data_origin,
		created_ew_dttm = farBookChapter.created_ew_dttm,
		lastupd_dw_dttm = farBookChapter.lastupd_dw_dttm,
		batch_sid = farBookChapter.batch_sid )

	return tgtFarBookChapter
