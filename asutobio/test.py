from app.connectdb import EtlConnections


###############################################################################
# application connection manager:
# 	imports connections to source and target, pass in the application scope
#	and the manager scopes how the application will be used.
asuDwPsAppRun = EtlConnections("asutobio")


try:

	sesSource = asuDwPsAppRun.getSourceSession()
	sesTarget = asuDwPsAppRun.getTargetSession()


	import asudwDepartmentsToBioPs

	srcDepartments = asudwDepartmentsToBioPs.getSourceDepartmentsData( sesSource )

	iDepartments = 1
	for srcDepartment in srcDepartments:

		addDepartment = asudwDepartmentsToBioPs.processDepartmentsData( srcDepartment )
		sesTarget.add( srcDepartment )

		if iDepartments % 1000 == 0:
			try:
				sesTarget.flush()
			except Exception as e:
				sesTarget.rollback()
				raise e

		iDepartments += 1

	try:
		sesTarget.commit()
	except Exception as e:
		sesTarget.rollback()
		raise e


except Exception as e:
	raise e
finally:
	sesTarget.close()
	sesSource.close()

	asuDwPsAppRun.cleanUp()


# from app.connectdb import *
# from models.biopsmodels import BioPsPeople
# e = etlconnections("bioetl")

# sess = e.getSourceSession()
# # print help(sess)
# print dir(sess)



# for i in sess.query( BioPsPeople ).first():
# 	print i


# e.cleanUp

#######################################################################################
# from sqlalchemy import *
# import cx_Oracle
# from sshtunnels import *


# # import these configs from some place else...
# localPort = 1521
# localServer = "127.0.0.1"

# sourceDbServer = "ocmdev2.asu.edu"
# sourceDbPort = 1521

# # identityfile = "/home/primusdj/.ssh/id_rsa"
# sshUser = "primusdj"
# sshServer = "biodb.biodesign.asu.edu"


# get the ssh tunnel open
# ssh = SshTunnels()
# ssh.createSshTunnel(localPort, sourceDbServer, sourceDbPort, sshUser, sshServer)

#######################################################################################
# from asutobio.models.biopsmodels import BioPs

#######################################################################################
from sqlalchemy import *
import cx_Oracle
from sqlalchemy.engine import reflection
from models.asudwpsmodels import *


sourceDbUser = "ASU_BDI_EXTRACT_APP"
sourceDbPw = "np55adW_G1_Um-ii"
sourceDbNetServiceName = "ASUPMDEV"
engSourceString = 'oracle+cx_oracle://%s:%s@%s' % (sourceDbUser, sourceDbPw, sourceDbNetServiceName)
engineSource = create_engine(engSourceString, echo=True)

conn = engineSource.connect()

insp = reflection.Inspector.from_engine(engineSource)
# #  In python terminal... going to test the "raw sql statements"...

for tbl in insp.get_table_names('asudw'):
    print tbl

# ## asudw
# # far_conferenceproceedings
# # toad_plan_table
# # far_authoredbooks
# # far_editedbooks
# # far_bookchapters
# # far_faculty
# # far_facultydepartment
# # far_communityservices
# # far_honors
# # far_bookreviews
# # far_monetaryawardpurposes
# # far_monetaryawards
# # far_professionalservices
# # far_shortstories
# # far_universityservices
# # far_evaluations
# # far_encyclopediaarticles
# # far_mentoring
# # far_nonrefereedarticles
# # far_refereedarticles
# ## sysadm
# # ps_job
# # ps_dept_tbl
# # ps_jobcode_tbl
# ## directory
# # phone   					-- done
# # student_plan 				--
# # student_subplan			--
# # address					-- done
# # person 					-- done
# # job 						-- done
# # subaffiliation 			-- done
# # student_enrollment
# # principal

# #list of person columns:
for col in insp.get_columns('far_evaluations', schema='asudw'):
	print col


# sql = "SELECT * FROM DIRECTORY.PERSON WHERE asurite_id = 'primusdj' AND ROWNUM <=10"

# sql = "SELECT MAX(LENGTH(empl_rcd)) FROM DIRECTORY.JOB WHERE ROWNUM <=10"
# sql = "SELECT MAX(empl_rcd) FROM DIRECTORY.JOB WHERE 1=1"

# sql = "SELECT j.emplid, j.effdt, j.action_dt, j.job_entry_dt, j.dept_entry_dt, j.position_entry_dt, j.grade_entry_dt, j.step_entry_dt, j.union_fee_start_dt, j.union_fee_end_dt, j.union_seniority_dt, j.entry_date, j.lbr_fac_entry_dt, j.force_publish, j.hire_dt, j.last_hire_dt, j.termination_dt, j.asgn_start_dt, j.lst_asgn_start_dt, j.asgn_end_dt, j.last_date_worked, j.expected_return_dt, j.expected_end_date  SYSADM.PS_JOB j INNER JOIN DIRECTORY.PERSON p ON (j.emplid=p.emplid) WHERE p.asurite_id='primusdj'"
	
# sql = "SELECT D.DEPTID, D.DESCR, D.EFFDT, D.DESCRSHORT, D.COMPANY, D.LOCATION, D.SETID, D.SRC_SYS_ID, D.EFF_STATUS,  LISTAGG(D.DESCR, ': ') WITHIN GROUP (ORDER BY D.EFFDT) OVER (PARTITION BY D.DEPTID) dept_titles, ROW_NUMBER() OVER (PARTITION BY D.DEPTID ORDER BY D.EFFDT desc) rn FROM SYSADM.PS_DEPT_TBL D WHERE D.DESCR LIKE '%Biodesign%'"

# # the raw sql we want to use....
# sql = "select u.emplid from ( SELECT job.emplid \
# FROM ( \
# 	SELECT  \
# 		emplid, deptid, jobcode, effdt, action, \
# 		ROW_NUMBER() OVER (PARTITION BY emplid, main_appt_num_jpn ORDER BY effdt DESC) rn \
# 	FROM SYSADM.PS_JOB \
# ) job \
# INNER JOIN ( \
# 	SELECT deptid \
# 	FROM SYSADM.PS_DEPT_TBL \
# 	WHERE descr like '%Biodesign%' \
# 	GROUP BY deptid \
# ) dept ON (job.deptid=dept.deptid) \
# WHERE job.rn = 1 AND job.action NOT IN ('TER','RET') \
# UNION \
# SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (aff.deptid=dept.deptid) INNER JOIN DIRECTORY.PERSON P ON ( aff.emplid=P.emplid ) WHERE P.asurite_id IS NOT NULL AND aff.subaffiliation_code IN ('BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL')
# ) as u WHERE u.emplid = 1000091891





# """looking for the reason i'm not in the etl..."""
# # job history with window (IN)
# sql = "SELECT emplid, deptid, jobcode, effdt, action, ROW_NUMBER() OVER (PARTITION BY emplid, main_appt_num_jpn ORDER BY effdt DESC) rn FROM SYSADM.PS_JOB WHERE emplid = 1000091891"
# ('1000091891', 'E0802004', '191706', datetime.datetime(2014, 12, 15, 0, 0), 'XFR', 1)
# # job history joined against the departments filter.
# sql = "SELECT job.emplid FROM ( SELECT  emplid, deptid, jobcode, effdt, action, ROW_NUMBER() OVER (PARTITION BY emplid, main_appt_num_jpn ORDER BY effdt DESC) rn FROM SYSADM.PS_JOB ) job INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (job.deptid=dept.deptid) WHERE job.rn = 1 AND job.action NOT IN ('TER','RET') AND job.emplid=1000091891"
# ('1000091891',)
# # now look into the sub affiliations:
# sql = "SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (aff.deptid=dept.deptid) INNER JOIN DIRECTORY.PERSON P ON ( aff.emplid=P.emplid ) WHERE P.asurite_id IS NOT NULL AND aff.emplid = 1000091891 AND aff.subaffiliation_code IN ('BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL')"
# None
# # look just at the subaffiliations:
# sql = "SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff WHERE aff.emplid = 1000091891 "
# ('1000091891',)
# # subaffiliations with groups:
# sql = "SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (aff.deptid=dept.deptid)  WHERE aff.emplid = 1000091891"
# None

# # Now, why does the union not return my id in the list?
# sql = "SELECT u.emplid from ( SELECT job.emplid FROM ( SELECT  emplid, deptid, jobcode, effdt, action, ROW_NUMBER() OVER (PARTITION BY emplid, main_appt_num_jpn ORDER BY effdt DESC) rn FROM SYSADM.PS_JOB ) job INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (job.deptid=dept.deptid) WHERE job.rn = 1 AND job.action NOT IN ('TER','RET') UNION SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (aff.deptid=dept.deptid) INNER JOIN DIRECTORY.PERSON P ON ( aff.emplid=P.emplid ) WHERE P.asurite_id IS NOT NULL AND aff.subaffiliation_code IN ('BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL') ) u ORDER BY u.emplid"

# sql = "SELECT p.emplid FROM DIRECTORY.PERSON p JOIN ( SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (aff.deptid=dept.deptid) WHERE aff.subaffiliation_code IN ('BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL') ) thisaff ON ( p.emplid=thisaff.emplid ) WHERE p.asurite_id IS NULL"

# sql = "SELECT emplid FROM DIRECTORY.PERSON WHERE asurite_id IS NULL and ROWNUM <= 5"

# sql = "SELECT aff.emplid FROM DIRECTORY.SUBAFFILIATION aff INNER JOIN ( SELECT deptid FROM SYSADM.PS_DEPT_TBL WHERE descr like '%Biodesign%' GROUP BY deptid ) dept ON (aff.deptid=dept.deptid) INNER JOIN DIRECTORY.PERSON P ON ( aff.emplid=P.emplid ) WHERE aff.subaffiliation_code IN ('BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL')"

# for row in conn.execute(sql):
# 	print row



# # sqlalchemy sql expression language equivelent:
# # Get the GROUPS list...
# s = select()

# for row in engineSource.execute(sql):
# 	print row







# sourceConnection.close()

# ssh.closeSshTunnels()
