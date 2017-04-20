import boto3
import time
from pyodbc import connect
from ipdw_connect import IPDW_PROD02

from jira import JIRA

# Logging in to sqs service
# http://russell.ballestrini.net/setting-region-programmatically-in-boto3/
# http://stackoverflow.com/questions/33297172/boto3-error-botocore-exceptions-nocredentialserror-unable-to-locate-credential

access_key = "AKIAJI4ZDH6A2LF4PFLA"
secret_access_key = "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"
region = "us-east-1"

sqs = boto3.resource('sqs',
                     aws_access_key_id=access_key,
                     aws_secret_access_key=secret_access_key,
                     region_name=region)

# Setting to The_Data_Warehouse queue
queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/732116115478/The_Data_Warehouse')


# Example: sending receiving messages
# http://boto3.readthedocs.io/en/latest/guide/sqs.html

response = q.send_message(MessageBody='This is a new message.')
q.delete_messages()
messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=10)
for message in messages:
    print(message.body)
messages[0].message_id
message.body
messages[0].body

# exec spIPD_Alexa_Testing @Quarter = 'Q1 2014', @Quarter2 = 'Q3 2012'

# How to process messages:

# Read message and delete
# Perform action
# Write new message
test = 0

while test < 4:
    test += 1
    print(test)

# Logic for looping
# http://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds-in-python
def executesomething():
    #code here
    time.sleep(60)

while True:
    executesomething()
    break

#

# IPDW connectivity

# Execute a store procedure with a variable
# http://stackoverflow.com/questions/9419276/calling-a-stored-procedure-python
#this will pass params just has them hard coded for testing
conn = connect(
    r'DRIVER={SQL Server};'
    r'SERVER=GRDDWMRTWHQIPDW;'
    r'DATABASE=IPDW_Prod02;'
    r'Trusted_connection=True'
)
conn.execute("exec dbo.UpdateCheck_In @room = '400', @building = 'PE', @department = 307, @global_id = 'bacon', @tag = '120420'")
conn.commit()


# SPs list
# Job_ID, Job_Name, Step_ID, Step_Name, Failed_Message, Assignee_Op_ID, Assignee_Name, Reporter_Op_ID, Reporter_Name



# Connecting to Jira
options = {"server": "https://jira1.cerner.com"}
jira = JIRA(options, basic_auth=(username, password))

# Create issue, create message body, assign assignee and reporter
