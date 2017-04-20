from ipdw_connect import IPDW_TEST02
import pyodbc


def get_error(job_name):
    sql = "SELECT Failed_Message FROM vIPD_Alexa_Job_Failures WHERE job_name = '" + job_name + "'"
    data_connection = IPDW_TEST02()
    try:
        data = data_connection.dataframe(sql)
        failed_message = data.loc[0, "Failed_Message"]
        if "Could not" in failed_message:
            result = "Could not" + failed_message.partition("Could not")[-1].partition(".")[0]
        elif "Failed to decrypt protected XML node" in failed_message:
            result = "I recommend logging a JIRA because there seems to be a password missing."
        else:
            result = "It's too complicated to read, how about I log a Jira?"
        return result
    except pyodbc.ProgrammingError:
        return "Actually, that job did not recently fail."

