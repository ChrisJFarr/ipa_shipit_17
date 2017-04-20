import boto3
import time
from ipdw_connect import IPDW_TEST02
import pyodbc

def get_error(job_name):
    sql = "SELECT Failed_Message FROM vIPD_Alexa_Job_Failures WHERE job_name = '" + job_name + "'"
    data_connection = IPDW_TEST02()
    try:
        data = data_connection.dataframe(sql)
        return "The error message for " + job_name + " is " + data["Failed_Message"]
    except pyodbc.ProgrammingError:
        return "That job did not fail"


print(get_error("Alexa Test Job"))
