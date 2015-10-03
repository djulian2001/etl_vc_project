from sqlalchemy import *
import cx_Oracle
from sqlalchemy.engine import reflection
from models.asudwpsmodels import *


sourceDbUser = "ASU_BDI_EXTRACT_APP"
sourceDbPw = "np55adW_G1_Um-ii"
sourceDbNetServiceName = "ASUPMDEV"
engSourceString = 'oracle+cx_oracle://%s:%s@%s' % (sourceDbUser, sourceDbPw, sourceDbNetServiceName)
engineSource = create_engine(engSourceString, echo=True)

AsuDwPs.metadata.bind = engineSource

SrcSession = scoped_session(sessionmaker(bind=engineSource))

true, false = literal(True), literal(False)

src_session = SrcSession()

# sql = text("SELECT job.emplid \
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
# SELECT aff.emplid \
# FROM DIRECTORY.SUBAFFILIATION aff \
# INNER JOIN ( \
# 	SELECT deptid \
# 	FROM SYSADM.PS_DEPT_TBL \
# 	WHERE descr like '%Biodesign%' \
# 	GROUP BY deptid \
# ) dept ON (aff.deptid=dept.deptid) \
# WHERE aff.subaffiliation_code IN ('BDAF','BDRP','BDAS','BDFC','BDAG','BAPD','BVIP','BDAU','BDHV','NCON','BDEC','NVOL')")

# sql = src_session.query(AsuDwPsDepartments).where(AsuDwPsDepartments.descr.like("%Biodesign%")).first()

theseFilters = AsuPsBioFilters(src_session)

print theseFilters.getBiodesignEmplidList(False)

# for this in emplid_list:
# 	print this.emplid

# id_list = src_session.execute(sql).fetchall()
# for an_id in id_list:
# 	print an_id