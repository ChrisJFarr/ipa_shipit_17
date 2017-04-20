import boto3
import time
from ipdw_connect import IPDW_TEST02


def get_job_failures():
    view_sql = "SELECT * FROM vIPD_Alexa_Job_Failures"

    data_connection = IPDW_TEST02()

    data = data_connection.dataframe(view_sql)

    data.to_csv("latest_failed.csv", index=None)
    # Pull in data from view
    # If there is anything returned, store as a csv
    # Create message
    # "The following jobs have failed." + job (create loop with concat)

    failed_jobs = data["Job_Name"]
    start_message = "The following jobs have failed. "
    job_index = 0
    message = ""
    for job in data["Job_Name"]:
        job_index += 1
        if job_index == 1:
            message = start_message + job
        elif job_index != len(failed_jobs):
            message = message + ", " + job
        elif job_index == len(failed_jobs) & len(failed_jobs) == 2:
            message = message + ", and " + job + "."
    return message

