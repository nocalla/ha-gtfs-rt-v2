"""
Script for quicker and easier testing of GTFS-RT-V2 outside of Home Assistant.
Usage: test.py -f <yaml file> -d INFO|DEBUG { -l <outfile log file> }

<yaml file> contains the sensor configuration from HA.
See test_translink.yaml for example
<output file> is a text file for output
"""
import argparse
import cProfile
import logging
import pstats
import sys
import time
from io import StringIO

import yaml
from sensor import PLATFORM_SCHEMA, setup_platform
from utils import _LOGGER
from voluptuous import Invalid

sys.path.append("lib")


def add_devices(sensors: list):
    """Placeholder function to mock up Homeassistant function"""
    return


def get_arguments():
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
    return args


def validate_config(file: str) -> dict:
    with open(file, "r") as test_yaml:
        input_config = yaml.safe_load(test_yaml)
        input_config["platform"] = "platform"
    configuration = dict()
    try:
        configuration = PLATFORM_SCHEMA(input_config)
        _LOGGER.info("Input file configuration is valid.")
    except Invalid as se:
        _LOGGER.error(input_config)
        logging.error(f"Input file configuration invalid: {se}")
    return configuration


def update_sensors(sensors: list, config: dict) -> list:
    if len(sensors) == 0:
        sensors = setup_platform("", config, add_devices, None)
    else:
        for sensor in sensors:
            sensor.update()
    return sensors


def main():
    args = get_arguments()
    configuration = validate_config(args["file"])
    _LOGGER.info(configuration)

    sensors = list()
    loop_str = "\nLooping is now active - press Ctrl & C to cancel."
    print(loop_str)
    _LOGGER.info(loop_str)
    profiler = cProfile.Profile()

    while True:
        profiler.enable()
        sensors = update_sensors(sensors, configuration)

        profiler.disable()
        stats_file = StringIO()
        stats = (
            pstats.Stats(profiler, stream=stats_file)
            .strip_dirs()
            .sort_stats("cumtime")
        )
        stats.print_stats(20)
        _LOGGER.info(
            "\tCode profiling stats for this iteration:\n"
            f"{stats_file.getvalue()}"
        )

        _LOGGER.info("Waiting before looping...")
        time.sleep(60)  # test out repeated polling


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _LOGGER.error("Interrupted manually.")
