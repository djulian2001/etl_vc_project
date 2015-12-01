from models.asudwpsmodels import AsuDwPsFarBookReviews, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarBookReviews

from sharedProcesses import hashThisList

# the data pull
def getSourceFarBookReviewsData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarBookReviews model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarBookReviews ).join(
			farEvals, AsuDwPsFarBookReviews.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarBookReviews.ispublic !='N' ).all()
	
# the data load
def processFarBookReviewData( farBookReview ):
	"""
		Process an AsuDwPsFarBookReviews object and prepare it for insert into the target BioPsFarBookReviews table
		@return: the sa add object 
	"""
	
	farBookReviewList = [
		farBookReview.bookreviewid,
		farBookReview.src_sys_id,
		farBookReview.evaluationid,
		farBookReview.bookauthors,
		farBookReview.booktitle,
		farBookReview.journalname,
		farBookReview.publicationstatuscode,
		farBookReview.journalpages,
		farBookReview.journalpublicationyear,
		farBookReview.journalvolumenumber,
		farBookReview.webaddress,
		farBookReview.additionalinfo,
		farBookReview.dtcreated,
		farBookReview.dtupdated,
		farBookReview.userlastmodified,
		farBookReview.ispublic,
		farBookReview.activityid,
		farBookReview.load_error,
		farBookReview.data_origin,
		farBookReview.created_ew_dttm,
		farBookReview.lastupd_dw_dttm,
		farBookReview.batch_sid ]

	farBookReviewHash = hashThisList( farBookReviewList )

	tgtFarBookReview = BioPsFarBookReviews(
		source_hash = farBookReviewHash,
		bookreviewid = farBookReview.bookreviewid,
		src_sys_id = farBookReview.src_sys_id,
		evaluationid = farBookReview.evaluationid,
		bookauthors = farBookReview.bookauthors,
		booktitle = farBookReview.booktitle,
		journalname = farBookReview.journalname,
		publicationstatuscode = farBookReview.publicationstatuscode,
		journalpages = farBookReview.journalpages,
		journalpublicationyear = farBookReview.journalpublicationyear,
		journalvolumenumber = farBookReview.journalvolumenumber,
		webaddress = farBookReview.webaddress,
		additionalinfo = farBookReview.additionalinfo,
		dtcreated = farBookReview.dtcreated,
		dtupdated = farBookReview.dtupdated,
		userlastmodified = farBookReview.userlastmodified,
		ispublic = farBookReview.ispublic,
		activityid = farBookReview.activityid,
		load_error = farBookReview.load_error,
		data_origin = farBookReview.data_origin,
		created_ew_dttm = farBookReview.created_ew_dttm,
		lastupd_dw_dttm = farBookReview.lastupd_dw_dttm,
		batch_sid = farBookReview.batch_sid )

	return tgtFarBookReview
