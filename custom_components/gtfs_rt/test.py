"""
Script for quicker and easier testing of GTFS-RT-V2 outside of Home Assistant.
Usage: test.py -f <yaml file> -d INFO|DEBUG { -l <outfile log file> }

<yaml file> contains the sensor configuration from HA.
See test_translink.yaml for example
<output file> is a text file for output
"""
import argparse
import logging
import sys

import yaml
from schema import Optional, Schema, SchemaError
from sensor import (
    CONF_API_KEY,
    CONF_DEPARTURES,
    CONF_DIRECTION_ID,
    CONF_GTFS_URL,
    CONF_ICON,
    CONF_ROUTE,
    CONF_ROUTE_DELIMITER,
    CONF_ROUTE_NAME,
    CONF_SERVICE_TYPE,
    CONF_STOP_CODE,
    CONF_STOP_ID,
    CONF_TRIP_UPDATE_URL,
    CONF_VEHICLE_POSITION_URL,
    CONF_X_API_KEY,
    setup_platform,
)

sys.path.append("lib")
_LOGGER = logging.getLogger(__name__)

CONF_NAME = "name"


def add_devices(sensors: list):
    """Placeholder function to mock up Homeassistant function"""
    return


PLATFORM_SCHEMA = Schema(
    {
        CONF_TRIP_UPDATE_URL: str,
        Optional(CONF_API_KEY): str,
        Optional(CONF_X_API_KEY): str,
        Optional(CONF_VEHICLE_POSITION_URL): str,
        Optional(CONF_ROUTE_DELIMITER): str,
        CONF_GTFS_URL: str,
        CONF_DEPARTURES: [
            {
                CONF_NAME: str,
                CONF_STOP_ID: str,
                CONF_ROUTE: str,
                Optional(CONF_DIRECTION_ID): str,
                Optional(CONF_SERVICE_TYPE): str,
                Optional(CONF_ICON): str,
                CONF_ROUTE_NAME: str,
                CONF_STOP_CODE: str,
            }
        ],
    }
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test script for ha-gtfs-rt-v2"
    )
    parser.add_argument(
        "-f", "--file", dest="file", help="Config file to use", metavar="FILE"
    )
    parser.add_argument(
        "-l", "--log", dest="log", help="Output file for log", metavar="FILE"
    )
    parser.add_argument(
        "-d",
        "--debug",
        dest="debug",
        help="Debug level: INFO (default) or DEBUG",
    )
    args = vars(parser.parse_args())

    if args["file"] is None:
        raise ValueError("Config file spec required.")
    if args["debug"] is None:
        DEBUG_LEVEL = "INFO"
    elif args["debug"].upper() == "INFO" or args["debug"].upper() == "DEBUG":
        DEBUG_LEVEL = args["debug"].upper()
    else:
        raise ValueError("Debug level must be INFO or DEBUG")
    if args["log"] is None:
        logging.basicConfig(level=DEBUG_LEVEL)
    else:
        logging.basicConfig(
            filename=args["log"], filemode="w", level=DEBUG_LEVEL
        )

    with open(args["file"], "r") as test_yaml:
        configuration = yaml.safe_load(test_yaml)
    try:
        PLATFORM_SCHEMA.validate(configuration)
        logging.info("Input file configuration is valid.")
        setup_platform("", configuration, add_devices, None)

    except SchemaError as se:
        logging.info("Input file configuration invalid: {}".format(se))
