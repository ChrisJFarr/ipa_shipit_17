import boto3
import time
import json
from get_job_failures import get_job_failures
from random import randint
from restart_job import restart_job
from get_error import get_error

access_key = "AKIAJI4ZDH6A2LF4PFLA"
secret_access_key = "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"
region = "us-east-1"


def test_for_messages_and_reply():

    while True:

        sqs = boto3.resource('sqs',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_access_key,
                             region_name=region)

        # Setting to The_Data_Warehouse queue
        queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/732116115478/The_Data_Warehouse')

        # Test for messages on loop

        # When message exists, delete message
        message = None

        # Loop every 3-5 seconds while no messages exist
        while message is None:
            message = test_for_messages(queue)

        message.delete()

        message_received = message.body

        response_dict = json.loads(message_received)
        response_name = response_dict["request"]["intent"]["name"]

        reply = ["I don't know what you're talking about.",
                 "Could you try to speak up?",
                 "Truncated all IPDW....just kidding. What did you say?",
                 "Hmmm I'm not sure if I read you."]
        if response_name == "turnOff":
            break
        elif response_name == "getJobFailures":
            reply = get_job_failures()
        elif response_name == "restartJob":
            try:
                parameter = response_dict['request']['intent']['slots']['job']['value']
                reply = restart_job(parameter)
            except:
                reply = "Hmmm there seems to be something wrong."
        elif response_name == "getError":
            try:
                parameter = response_dict['request']['intent']['slots']['job']['value']
                reply = get_error(parameter)
            except:
                reply = "Hmmm there seems to be something wrong."

        # Respond with new message
        if len(reply) == 4:
            reply = reply[randint(0, 3)]

        queue.send_message(MessageBody=reply)




def test_for_messages(queue):
    time.sleep(1)
    # code here
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=10)
    try:
        message = messages[0]
    except IndexError:
        message = None

    return message


response = test_for_messages_and_reply()
