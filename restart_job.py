import pandas as pd
import pyodbc
from ipdw_connect import IPDW_TEST02


def restart_job(job_name):

    sql = "EXEC msdb.dbo.sp_start_job @job_name = '%s', @server_name='GRDDWMRTWHQIPDW'" % job_name
    ipdw_test = IPDW_TEST02()
    try:
        ipdw_test.execute(sql)
        result = 'I have successfully restarted the job %s.' % job_name
    except pyodbc.ProgrammingError:
        result = 'That job does not exist.'
    return result

