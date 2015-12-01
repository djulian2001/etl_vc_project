from app.connectdb import EtlConnections

###############################################################################
# application connection manager:
# 	imports connections to source and target, pass in the application scope
#	and the manager scopes how the application will be used.
asuDwPsAppRun = EtlConnections("asutobio")


try:
	sesSource = asuDwPsAppRun.getSourceSession()
	sesTarget = asuDwPsAppRun.getTargetSession()


###############################################################################
# Extract the oracle person table and cut up into:
#	mysql:
#		people
#		person_externallinks
#		person_webprofile, 
# 	


	# import asudwPeopleToBioPs

	# srcPersons = asudwPeopleToBioPs.getSourcePerson( sesSource )

	# iPerson = 1
	# for srcPerson in srcPersons:

	# 	addPerson = asudwPeopleToBioPs.processPersonData( srcPerson )
	# 	sesTarget.add( addPerson )

	# 	addPersonExtLinks = asudwPeopleToBioPs.processPersonExternalLinksData( srcPerson )
	# 	if addPersonExtLinks:
	# 		sesTarget.add( addPersonExtLinks )

	# 	addPersonWebProfile = asudwPeopleToBioPs.processPersonWebProfile( srcPerson )
	# 	if addPersonWebProfile:
	# 		sesTarget.add( addPersonWebProfile )

	# 	if iPerson % 333 == 0:
	# 		try:
	# 			sesTarget.flush()
	# 		except Exception as e:
	# 			# sqlalchemy.orm.exc.FlushError might be a better...
	# 			sesTarget.rollback()
	# 			raise e
		
	# 	iPerson += 1

	# try:
	# 	sesTarget.commit()
	# except Exception as e:
	# 	sesTarget.rollback()
	# 	raise e

#
# end of for srcPersons
###############################################################################


###############################################################################
# Extract the oracle table and load it into a mysql table:
# 	oracle:
#		ASUDW.FAR_EVALUATIONS
#	mysql:
#		far_evaluations
#

	try:
	
		# import asudwFarEvaluationToBioPs
		
		# srcFarEvaluation = asudwFarEvaluationToBioPs.getSourceFarEvaluationsData( sesSource )

		# iFarEvaluation = 1
		# for farEvaluation in srcFarEvaluation:
			
		# 	addFarEvaluation = asudwFarEvaluationToBioPs.processFarEvaluationData( farEvaluation )
			
		# 	sesTarget.add( addFarEvaluation )

		# 	if iFarEvaluation % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarEvaluation += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e
		print "hello world;"

	except Exception as e:
			raise e
	else:
	
	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_CONFERENCEPROCEEDINGS
	#	mysql:
	#		far_conferenceproceedings
	#

		# import asudwFarConferenceProceedingToBioPs
		
		# srcFarConferenceProceeding = asudwFarConferenceProceedingToBioPs.getSourceFarConferenceProceedingsData( sesSource )

		# iFarConferenceProceeding = 1
		# for farConferenceProceeding in srcFarConferenceProceeding:
			
		# 	addFarConferenceProceeding = asudwFarConferenceProceedingToBioPs.processFarConferenceProceedingData( farConferenceProceeding )
			
		# 	sesTarget.add( addFarConferenceProceeding )

		# 	if iFarConferenceProceeding % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarConferenceProceeding += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e

	#
	# end of for srcFarConferenceProceeding
	###############################################################################
	
	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_AUTHOREDBOOKS
	#	mysql:
	#		far_authoredbooks
	#

		# import asudwFarAuthoredBookToBioPs
		
		# srcFarAuthoredBook = asudwFarAuthoredBookToBioPs.getSourceFarAuthoredBooksData( sesSource )

		# iFarAuthoredBook = 1
		# for farAuthoredBook in srcFarAuthoredBook:
			
		# 	addFarAuthoredBook = asudwFarAuthoredBookToBioPs.processFarAuthoredBookData( farAuthoredBook )
			
		# 	sesTarget.add( addFarAuthoredBook )

		# 	if iFarAuthoredBook % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarAuthoredBook += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e

	#
	# end of for srcFarAuthoredBook
	###############################################################################


	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_REFEREEDARTICLES
	#	mysql:
	#		far_refereedarticles
	#

		# import asudwFarRefereedarticleToBioPs
		
		# srcFarRefereedarticle = asudwFarRefereedarticleToBioPs.getSourceFarRefereedarticlesData( sesSource )

		# print type(srcFarRefereedarticle)

		# iFarRefereedarticle = 1
		# for farRefereedarticle in srcFarRefereedarticle:

		# 	addFarRefereedarticle = asudwFarRefereedarticleToBioPs.processFarRefereedarticleData( farRefereedarticle )
			
		# 	sesTarget.add( addFarRefereedarticle )

		# 	if iFarRefereedarticle % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarRefereedarticle += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e

	#
	# end of for srcFarRefereedarticle
	###############################################################################


	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_NONREFEREEDARTICLES
	#	mysql:
	#		far_nonrefereedarticles
	#

		# import asudwFarNonrefereedarticleToBioPs
		
		# srcFarNonrefereedarticle = asudwFarNonrefereedarticleToBioPs.getSourceFarNonrefereedarticlesData( sesSource )

		# iFarNonrefereedarticle = 1
		# for farNonrefereedarticle in srcFarNonrefereedarticle:
			
		# 	addFarNonrefereedarticle = asudwFarNonrefereedarticleToBioPs.processFarNonrefereedarticleData( farNonrefereedarticle )
			
		# 	sesTarget.add( addFarNonrefereedarticle )

		# 	if iFarNonrefereedarticle % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarNonrefereedarticle += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e

	#
	# end of for srcFarNonrefereedarticle
	###############################################################################

	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_EDITEDBOOKS
	#	mysql:
	#		far_editedbooks
	#

		# import asudwFarEditedbookToBioPs
		
		# srcFarEditedbook = asudwFarEditedbookToBioPs.getSourceFarEditedbooksData( sesSource )

		# iFarEditedbook = 1
		# for farEditedbook in srcFarEditedbook:
			
		# 	addFarEditedbook = asudwFarEditedbookToBioPs.processFarEditedbookData( farEditedbook )
			
		# 	sesTarget.add( addFarEditedbook )

		# 	if iFarEditedbook % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarEditedbook += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e

	#
	# end of for srcFarEditedbook
	###############################################################################


	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_BOOKCHAPTERS
	#	mysql:
	#		far_bookchapters
	#

		# import asudwFarBookChapterToBioPs
		
		# srcFarBookChapter = asudwFarBookChapterToBioPs.getSourceFarBookChaptersData( sesSource )

		# iFarBookChapter = 1
		# for farBookChapter in srcFarBookChapter:
			
		# 	addFarBookChapter = asudwFarBookChapterToBioPs.processFarBookChapterData( farBookChapter )
			
		# 	sesTarget.add( addFarBookChapter )

		# 	if iFarBookChapter % 1000 == 0:
		# 		try:
		# 			sesTarget.flush()
		# 		except Exception as e:
		# 			sesTarget.rollback()
		# 			raise e

		# 	iFarBookChapter += 1

		# try:
		# 	sesTarget.commit()
		# except Exception as e:
		# 	sesTarget.rollback()
		# 	raise e

	#
	# end of for srcFarBookChapter
	###############################################################################



	###############################################################################
	# Extract the oracle table and load it into a mysql table:
	# 	oracle:
	#		ASUDW.FAR_BOOKREVIEWS
	#	mysql:
	#		far_bookreviews
	#

		import asudwFarBookReviewToBioPs
		
		srcFarBookReview = asudwFarBookReviewToBioPs.getSourceFarBookReviewsData( sesSource )

		iFarBookReview = 1
		for farBookReview in srcFarBookReview:
			
			addFarBookReview = asudwFarBookReviewToBioPs.processFarBookReviewData( farBookReview )
			
			sesTarget.add( addFarBookReview )

			if iFarBookReview % 1000 == 0:
				try:
					sesTarget.flush()
				except Exception as e:
					sesTarget.rollback()
					raise e

			iFarBookReview += 1

		try:
			sesTarget.commit()
		except Exception as e:
			sesTarget.rollback()
			raise e

	#
	# end of for srcFarBookReview
	###############################################################################


#
# end of for srcFarEvaluation
###############################################################################
		print "hello world;"

except Exception as e:
	raise e
finally:
	sesTarget.close()
	sesSource.close()

	asuDwPsAppRun.cleanUp()