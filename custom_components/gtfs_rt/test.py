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
import time

import yaml
from sensor import PLATFORM_SCHEMA, setup_platform
from utils import _LOGGER
from voluptuous import Invalid

sys.path.append("lib")


def add_devices(sensors: list):
    """Placeholder function to mock up Homeassistant function"""
    return


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
        input_config = yaml.safe_load(test_yaml)
        input_config["platform"] = "platform"

    try:
        configuration = PLATFORM_SCHEMA(input_config)
        _LOGGER.info("Input file configuration is valid.")
        _LOGGER.info(configuration)
        start_time = time.time()
        sensors = setup_platform("", configuration, add_devices, None)
        elapsed_time = time.time() - start_time
        _LOGGER.info(f"\nElapsed time: {elapsed_time:.2f} seconds")
        print("Looping underway- cancel loop with CTRL+C\n")
        while True:
            _LOGGER.info(
                "\nWaiting before looping (Cancel loop with CTRL+C)..."
            )
            time.sleep(60)  # test out repeated polling
            start_time = time.time()
            _LOGGER.info(f"\nUpdating sensors @ {start_time}...")
            for sensor in sensors:
                sensor.update()
            elapsed_time = time.time() - start_time
            _LOGGER.info(f"\nElapsed time: {elapsed_time:.2f} seconds")
    except KeyboardInterrupt:
        logging.info("Loop terminated manually.")
    except Invalid as se:
        _LOGGER.error(input_config)
        logging.error(f"Input file configuration invalid: {se}")
