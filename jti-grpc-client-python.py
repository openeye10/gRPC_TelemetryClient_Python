# ####################################################################
#
#
#   888888 88888888888 8888888
#       "88b     888       888
#        888     888       888
#        888     888       888
#        888     888       888
#        888     888       888
#        88P     888       888
#        888     888     8888888
#      .d88P
#    .d88P"
#   888P"
#    .d8888b.  8888888b.  8888888b.   .d8888b.
#   d88P  Y88b 888   Y88b 888   Y88b d88P  Y88b
#   888    888 888    888 888    888 888    888
#   888        888   d88P 888   d88P 888
#   888  88888 8888888P"  8888888P"  888
#   888    888 888 T88b   888        888    888
#   Y88b  d88P 888  T88b  888        Y88b  d88P
#    "Y8888P88 888   T88b 888         "Y8888P"
#
#
#
#    .d8888b.  888      8888888 8888888888 888b    888 88888888888
#   d88P  Y88b 888        888   888        8888b   888     888
#   888    888 888        888   888        88888b  888     888
#   888        888        888   8888888    888Y88b 888     888
#   888        888        888   888        888 Y88b888     888
#   888    888 888        888   888        888  Y88888     888
#   Y88b  d88P 888        888   888        888   Y8888     888
#    "Y8888P"  88888888 8888888 8888888888 888    Y888     888
#
#
#       > Script:  jti-grpc-client-python.py
#       > Author: Tech Mocha
#       > Company: Tech Mocha
#       > Version: 0.1
#       > Revision Date: 2018-09-19
#
# ####################################################################


'''
----------[  U S A G E   E X A M P L E S  ]----------

(0) INSTALL PREREQUISITE PACKAGES:

        > curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
        > sudo python get-pip.py
        > sudo pip install kafka-python
        > sudo pip install requests

'''

# ----------[ IMPORTS ]----------
import argparse
import calendar
import xml.etree
import xml.etree.ElementTree as ET
import grpc
import json
import logging
import requests
import sys
import time
import xml.dom.minidom

from kafka import KafkaConsumer
from kafka import KafkaProducer
from logging.handlers import TimedRotatingFileHandler
from protos_compiled import agent_pb2
from protos_compiled import agent_pb2_grpc
from protos_compiled import authentication_service_pb2
from protos_compiled import authentication_service_pb2_grpc



# ----------[ GLOBAL CONSTANTS ]----------
ARGPARSER = "JTI-GRPC-CLIENT ARGUMENTS"
COLLECTOR_ADDRESS = "10.49.123.198"
COLLECTOR_PORT = 50051

# MX2 - OpenConfig 0.0.0.10 - gRPC
#DEVICE_IP = "10.49.126.157"

# MX3 - OpenConfig 0.0.0.9 - Native & Syslog
DEVICE_IP = "10.49.114.76"

# Paul Abbott's Router
#DEVICE_IP = "10.49.123.198"
DEVICE_PASSWORD = ""
DEVICE_PORT = "50051"
DEVICE_USERNAME = ""
INVOKE_GET_OPERATIONAL_STATE_FLAG = False
INVOKE_GET_DATA_ENCODINGS_FLAG = False
INVOKE_GET_SUBSCRIPTIONS_FLAG = False
JTI_INTERFACES_SENSOR_SUBSTRING = "/interfaces/interface"
JTI_SYSLOG_SENSOR_SUBSTRING = "/junos/events"
KAFKA_IP = "10.49.114.238"
KAFKA_PORT = "9092"
KAFKA_TOPIC_JUNIPER = "juniper"
KAFKA_TOPIC_JUNIPER_INTERFACES = "juniper_interfaces"
KAFKA_TOPIC_JUNIPER_SYSLOG = "juniper_syslog"
LOGFILE = "log/jti_grpc_client.log"
SOUTHBOUND_KAFKA_FLAG = False

def enum(**named_values):
    return type('Enum', (), named_values)
VERBOSITY = enum(BRIEF='BRIEF', DETAIL='DETAIL', TERSE='TERSE')


# ----------[ GLOBAL VARIABLES ]----------
args = None
logger = None



# ----------[ FUNCTION 'main()' ]----------
def main(argv):
    # Explicitly refer to the global variables to avoid creating a local variable that shadows the outer scope.
    global args

    # STEP 1: Setup the logging so we can start to log debug info right away.
    setupLogging()
    logger.debug("\n\n\n--------------------[ JTI-GRPC-CLIENT: BEGIN EXECUTION ]--------------------")

    # STEP 2: Parse the argument list with which this script was invoked and set global variable 'args'.
    logger.debug("Number of arguments used in script invocation: " + str(len(argv)) + "\n")
    logger.info("Full argument list: " + str(argv) + "\n")
    #args = parseArguments()

    # STEP 3: Create an insecure channel to the gRPC server running on the router, and use the channel to create
    # the client stub
    device_ip_and_port = DEVICE_IP + ":" + DEVICE_PORT
    logger.info("Creating insecure channel to " + device_ip_and_port + " ...")
    grpc_channel = grpc.insecure_channel(device_ip_and_port)
    stub = agent_pb2_grpc.OpenConfigTelemetryStub(grpc_channel)
    logger.info("... Done!\n")

    # STEP 4: Get the Telemetry Agent operational states (this is one of the methods exposed via agent.proto).
    # As per the .proto file, use 0xFFFFFFFF for all subscription identifiers including agent-level operational stats.
    if INVOKE_GET_OPERATIONAL_STATE_FLAG:
        logger.info("Invoking 'getTelemetryOperationalState()' ...")
        get_oper_state_request = agent_pb2.GetOperationalStateRequest(subscription_id = 0xFFFFFFFF,
                                                                      verbosity = agent_pb2.VerbosityLevel.Value(VERBOSITY.BRIEF))
        get_oper_state_response = stub.getTelemetryOperationalState(get_oper_state_request)
        logger.info(str(get_oper_state_response))
        logger.info("... Done!\n")

    # STEP 5: Return the set of data encodings supported by the device for telemetry data.
    if INVOKE_GET_DATA_ENCODINGS_FLAG:
        logger.info("Invoking 'getDataEncodings()' ...")
        get_data_encoding_request = agent_pb2.DataEncodingRequest()
        get_data_encoding_reply = stub.getDataEncodings(get_data_encoding_request)
        logger.info(str(get_data_encoding_reply))
        logger.info("... Done!\n")

    # STEP 6: Get the list of current telemetry subscriptions from the target (this is one of the methods exposed via agent.proto).
    # As per the .proto file, use 0xFFFFFFFF for all subscription identifiers.
    if INVOKE_GET_SUBSCRIPTIONS_FLAG:
        logger.info("Invoking 'getTelemetrySubscriptions()' ...")
        get_subscriptions_request = agent_pb2.GetSubscriptionsRequest(subscription_id = 0xFFFFFFFF)
        get_subscriptions_reply = stub.getTelemetrySubscriptions(get_subscriptions_request)
        logger.info(str(get_subscriptions_reply))
        logger.info("... Done!\n")


    # STEP 7: The telemetrySubscribe() method requires a SubscriptionRequest object as an input, which in turn requires
    # a SubscriptionInput object and a list of Path objects as input ... assemble these various objects.

    # Setup Collector ...
    collector = agent_pb2.Collector(address=COLLECTOR_ADDRESS, port=COLLECTOR_PORT)
    logger.debug("Value of 'collector': " + str(collector))

    # Use Collector to setup SubscriptionInput ...
    subscription_input = agent_pb2.SubscriptionInput(collector_list=[collector])
    logger.debug("Value of 'subscription_input':\n" + str(subscription_input))

    # Setup Path ...
    #path = agent_pb2.Path(path="/junos/system/linecard/interface/", sample_frequency=5000)
    #path = agent_pb2.Path(path="/interfaces/interface[name='ge-0/0/0']/", sample_frequency=5000)
    #path = agent_pb2.Path(path="/interfaces/interface[name='ge-0/0/0']/state/", sample_frequency=5000)
    #path = agent_pb2.Path(path="/junos/events", sample_frequency=0)
    #path = agent_pb2.Path(path="/junos/events/event[id=\'UI_COMMIT\']", sample_frequency=0)
    #path = agent_pb2.Path(path="/components/", sample_frequency=5000)

    ## Multiple Sensor Subscriptions ...
    path1 = agent_pb2.Path(path="/interfaces/interface[name='ge-0/0/0']/state/", sample_frequency=5000)
    path2 = agent_pb2.Path(path="/junos/events/event[id=\'UI_COMMIT\']", sample_frequency=0)
    #path2 = agent_pb2.Path(path="/junos/events", sample_frequency=0)

    # Use Path(s) to setup path_list ...
    #path_list = [path]
    path_list = [path1, path2]
    logger.debug("Value of 'path_list':\n" + str(path_list))

    # Use SubscriptionInput and path_list to setup SubscriptionRequest ...
    subscription_request = agent_pb2.SubscriptionRequest(input=subscription_input,
                                                         path_list=path_list)
    logger.info("Value of 'subscription_request':\n" + str(subscription_request))


    # Define Kafka Endpoint
    producer = None
    if SOUTHBOUND_KAFKA_FLAG:
        bootstrap_server = KAFKA_IP + ":" + KAFKA_PORT
        logger.info("Value of 'bootstrap_server':" + bootstrap_server)

        # Connect to Kafka as a Producer
        logger.info("Connecting to Kafka as a Producer ...")
        producer = KafkaProducer(bootstrap_servers=bootstrap_server)
        logger.info("... Done!\n")


    # Launch telemetry subscription request ...
    for message in stub.telemetrySubscribe(subscription_request):
        # Print each telemetry message to console.
        print(message)

        # Parse message and assemble contents in JSON format in preparation for Kafka push.
        data = {}
        data['system_id'] = message.system_id
        data['component_id'] = message.component_id
        data['sub_component_id'] = message.sub_component_id
        data['path'] = message.path
        data['sequence_number'] = message.sequence_number
        data['timestamp'] = message.timestamp

        # The telemetry data returned is a list of key-value pairs, where the value can be one of the following
        # possible values: double_value, int_value, uint_value, sint_value, bool_value, str_value, bytes_value.
        kv_pairs = []
        for kv in message.kv:
            key = kv.key
            value_type = kv.WhichOneof('value')

            if value_type == "double_value":
                kv_pairs.append({
                    key: kv.double_value
                })
            if value_type == "int_value":
                kv_pairs.append({
                    key: kv.int_value
                })
            if value_type == "uint_value":
                kv_pairs.append({
                    key: kv.uint_value
                })
            if value_type == "sint_value":
                kv_pairs.append({
                    key: kv.sint_value
                })
            if value_type == "bool_value":
                kv_pairs.append({
                    key: kv.bool_value
                })
            if value_type == "str_value":
                kv_pairs.append({
                    key: kv.str_value
                })
            if value_type == "bytes_value":
                kv_pairs.append({
                    key: kv.bytes_value
                })

            data['kv_pairs'] = kv_pairs

        #data['key'] = 'value'
        # Encode the data in JSON and pretty-print it before firing it off to Kafka.
        json_data = json.dumps(data, indent=3)

        if SOUTHBOUND_KAFKA_FLAG:
            # Publish message to Kafka bus.
            # Route to an appropriate Kafka topic, based on the telemetry subscription path.
            logger.info("Pushing message to Kafka ...")
            if JTI_INTERFACES_SENSOR_SUBSTRING in message.path:
                #producer.send(KAFKA_TOPIC_JUNIPER, json_data)
                producer.send(KAFKA_TOPIC_JUNIPER_INTERFACES, json_data)
                #producer.send('juniper', json_data)
            elif JTI_SYSLOG_SENSOR_SUBSTRING in message.path:
                #producer.send(KAFKA_TOPIC_JUNIPER, json_data)
                producer.send(KAFKA_TOPIC_JUNIPER_SYSLOG, json_data)
                #producer.send("juniper", json_data)
            else:
                producer.send(KAFKA_TOPIC_JUNIPER, json_data)

            # Block until all async messages are sent.
            # Failing to do so may result in the Producer being killed before messages are actually delivered!
            # Note that send() operates asynchronously!
            producer.flush()
            logger.info("... Done!\n")



# TODO - WORK IN PROGRESS ...
# ----------[ FUNCTION 'parseArguments()' ]----------
def parseArguments():
    logger.info("FUNCTION 'parseArguments(): BEGIN")

    # Note: Because of the number and complexity of the arguments, we will not use single character option letters
    # (eg. -o).
    # Instead, to promote clarity, we will enforce the use of long options (eg. --arg1).

    # Create an 'ArgumentParser' object, which will hold all the info necessary to parse the CLI options.
    argumentParser = argparse.ArgumentParser(description=ARGPARSER)

    # Because we need to support multiple operations (ADD, DELETE, MODIFY) in one script, different operation types will
    # require different arguments.  We use ArgParse's 'subparser' functionality here.
    # Split the functionality into sub-commands: ADD, DELETE, MODIFY and associated specific arguments with sub-command.
    argumentSubParsers = argumentParser.add_subparsers(help="Commands")

    opType_Add_Parser = argumentSubParsers.add_parser("ADD")
    opType_Delete_Parser = argumentSubParsers.add_parser("DELETE")
    opType_Modify_Parser = argumentSubParsers.add_parser("MODIFY")

    # Add the individual argements for ADD ...
    opType_Add_Parser.add_argument('--arg1', required=True, help="Argument 1")


    # Add the individual argements for DELETE ...
    # The API allows us to decommission a service based on Service ID, Service Order Name, or External ID.
    # We will implement support for deletion based on Service Name and External ID, however these must be
    # mutually exclusive.  We use Argparse's 'add_mutually_exclusive_group()' to implement this.  We also set
    # 'required=True' on the mutually exclusive group to force the user to enter either Service Order Name or
    # External ID.
    mutuallyExclusiveGroup_Delete = opType_Delete_Parser.add_mutually_exclusive_group(required=True)
    mutuallyExclusiveGroup_Delete.add_argument('--serviceName', help="Service Name")
    mutuallyExclusiveGroup_Delete.add_argument('--externalId', help="External ID")

    # Explicitly refer to the global variable to avoid creating a local variable that shadows the outer scope.
    #global args
    # Convert argument strings to objects and assign them as attributes of a namespace.
    # Return the populated namespace.
    # For example, if the script was invoked with "python 219-main.py --arg1 'test1' --arg2=test2 --arg3 test3",
    # then 'argumentParser.parse_args()' will return "Namespace(arg1='test1', arg2='test2', arg3='test3')"
    #
    # For DELETE, because we allow either the '--externalId' or '--serviceName' options to be set, we need to check
    # which one was set by looking into 'args'.  For example, if the script was invoked with a value of "extId1" for
    # '--externalId' and nothing for '--serviceName', then the value of "args" will look like this:
    #           Namespace(externalId='extId1', serviceName=None)
    # So, when checking if an optional argument was set or not, we need to test whether it's value is None.
    arguments = argumentParser.parse_args()
    logger.debug("Value of 'args': " + str(arguments))

    logger.info("FUNCTION 'parseArguments(): END")
    return arguments



# ----------[ FUNCTION 'setupLogging()' ]----------
def setupLogging():
    # Explicitly refer to the global variable to avoid creating a local variable that shadows the outer scope.
    global logger
    # Create logger.
    logger = logging.getLogger(__name__)

    # Create a rotating log file handler based on how much time has elapsed.
    # In this case, rotate the log file every day, to a maximum of 5 days.
    logFileHandler = TimedRotatingFileHandler(LOGFILE,
                                              when="d",
                                              interval=1,
                                              backupCount=5)

    # Set logging level to DEBUG or INFO
    # Note: Need to explicitly set 'logger.setLevel()' or we just get an empty file.
    logger.setLevel(logging.DEBUG)
    logFileHandler.setLevel(logging.DEBUG)

    # Create logging format and add it to the log file handler.
    logFormatter = logging.Formatter('[%(asctime)s][%(filename)s][Func %(funcName)s()][Line %(lineno)d][%(levelname)s]: %(message)s')
    logFileHandler.setFormatter(logFormatter)

    # Add the log file handler to the logger.
    logger.addHandler(logFileHandler)

    return



# ----------[ ]----------
# Have the code only execute when the module is run directly as a program, and not have it
# execute when someone wants to import the module and invoke the functions themselves.
if __name__ == "__main__":
    # Note: 'sys.argv' is a list of strings representing the command-line arguments.
    # 'sys.argv[0]' always represents the script name. So we are really only interested in the arguments after the
    # script name.
    # We can retrieve the arguments after the script name as follows: 'sys.argv[1:]'
    # Even though we are using 'argparse' to parse the argument list, we should still log the contents of 'sys.argv[0]'
    # for debugging purposes.
    main(sys.argv[1:])
