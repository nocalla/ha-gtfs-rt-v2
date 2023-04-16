import datetime
import io
import zipfile
from collections import defaultdict

import pandas as pd
import requests
from utils import _LOGGER


class GTFSCache:
    """
    Class for saving daily static GTFS data to a cache dictionary.
    """

    def __init__(self) -> None:
        self.data = dict()

    def save(self, date: datetime.date, data: pd.DataFrame) -> None:
        """
        Saves data to a dictionary for later access using "date" as a key.

        :param date: The date to use as a dictionary key.
        :type date: datetime.date
        :param data: Data to be saved in the cache.
        :type data: pd.DataFrame
        """
        _LOGGER.debug(f"Saving static GTFS data from {date} to cache...")
        self.data = {date: data}

    def load(self, date: datetime.date) -> pd.DataFrame | None:
        """
        Returns previously-cached data from the cache dictionary using "date"
        as a key or else returns an empty dictionary if the date is not
        present in the dictionary keys.

        :param date: The date to use as a dictionary key.
        :type date: datetime.date
        :return: Previously-saved data from the cache dictionary or None.
        :rtype: pd.DataFrame | None
        """
        try:
            _LOGGER.debug(f"Loading cached static GTFS data from {date}...")
            return self.data[date]
        except KeyError:
            _LOGGER.debug(f"No static GTFS data from {date} found in cache...")
            return None


class StaticMasterGTFSInfo:
    """
    Class to control access to the static GTFS information available from the
    public transport service provider.
    """

    def __init__(self, url: str, CachedData: GTFSCache) -> None:
        """
        Creates the class object by first checking if there are cached data
        available and returning those data. If there is no data for today's
        date found in the cache, this class triggers downloading and creating
        the required dictionary.

        :param url: URL from which to download GTFS data.
        :type url: str
        :param CachedData: Previously-saved cache of data.
        :type CachedData: GTFSCache
        """
        date_today = datetime.date.today()
        cache = CachedData.load(date_today)
        if cache is None:
            _LOGGER.debug("Getting new static GTFS data...")
            dataframes = get_dataframes(url)
            cache = process_dataframes(dataframes)
            CachedData.save(date_today, cache)
        self.departure_info = cache


def process_dataframes(dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merges the provided Pandas dataframes as needed and return a single Stop
    Schedule dataframe.

    :param dataframes: Dictionary mapping filenames to Pandas dataframes.
    :type dataframes: dict[str, pd.DataFrame]
    :return: Merged dataframe of stop schedule information.
    :rtype: pd.DataFrame
    """

    routes = dataframes["routes"]
    trips = dataframes["trips"]
    stop_times = dataframes["stop_times"]
    calendar = dataframes["calendar"]
    # reshape calendar_dates dataframe
    calendar_dates = (
        dataframes["calendar_dates"]
        .pivot_table(
            index="service_id",
            columns="date",
            values="exception_type",
            aggfunc="first",
        )
        .iloc[:, 1:-1]
        .add_prefix("exc_")
    )

    # merge dataframes

    _LOGGER.debug("Merging Route and Agency dataframes...")
    routes = pd.merge(routes, dataframes["agency"], on="agency_id", how="left")
    _LOGGER.debug("Merging stop and stoptimes dataframes...")
    stops = pd.merge(stop_times, dataframes["stops"], on="stop_id", how="left")
    _LOGGER.debug("Converting arrival and departure times to time deltas...")
    stops["scheduled_arrival_time"] = pd.to_timedelta(
        stops["arrival_time"]
    ).dt.total_seconds()
    stops["scheduled_departure_time"] = pd.to_timedelta(
        stops["departure_time"]
    ).dt.total_seconds()
    _LOGGER.debug("Merging calendar and calendar_dates dataframes...")
    calendar = pd.merge(calendar, calendar_dates, on="service_id", how="left")
    _LOGGER.debug("Merging trips and calendar dataframes...")
    calendar.columns = [
        f"cal_{col}" if col != "service_id" else col
        for col in calendar.columns
    ]
    trips = pd.merge(
        trips,
        calendar,
        on="service_id",
        how="left",
    )
    _LOGGER.debug("Merging routes and trips dataframes...")
    routes = pd.merge(trips, routes, on="route_id", how="left")
    _LOGGER.debug("Merging routes and stops dataframes...")
    stops = pd.merge(stops, routes, on="trip_id", how="left")
    _LOGGER.debug("Deleting empty columns...")
    stops = stops.dropna(how="all", axis=1)

    return stops


def get_dataframes(url: str) -> dict[str, pd.DataFrame]:
    """
    Downloads a zip file from "url", creates a Pandas dataframe based on each
    file within, and saves each dataframe to a dictionary using the filename
    (without file extension) as a key.

    :param url: URL to download zip file from.
    :type url: str
    :return: Dictionary mapping filename to Pandas dataframe for each file.
    :rtype: dict[str, pd.DataFrame]
    """
    # Make a GET request to the URL
    _LOGGER.info("Requesting GTFS static data...")
    response = requests.get(url, timeout=10, stream=True)
    response.raise_for_status()
    _LOGGER.info(f"Request zip file successful {response.status_code}")

    # Define an empty dictionary to store the Pandas DataFrames
    dataframes = dict()
    _LOGGER.info("Creating dataframes from source data...")

    target_files = [
        "routes.txt",
        "trips.txt",
        "stops.txt",
        "stop_times.txt",
        "agency.txt",
        "calendar.txt",
        "calendar_dates.txt",
    ]
    datatypes = defaultdict(
        lambda: "category",
        {
            "stop_sequence": "Int64",
            "stop_lat": float,
            "stop_lon": float,
        },
    )

    # Load the zip file into a ZipFile object
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        # Loop through each targeted file in the zip file
        for filename in [f for f in zip_file.namelist() if f in target_files]:
            # Extract the file from the zip file
            _LOGGER.debug(f"Creating dataframe from {filename}...")
            with zip_file.open(filename, "r") as file:
                # Create a Pandas DataFrame from the file
                df = pd.read_csv(file, dtype=datatypes)
                # Add the DataFrame to the dictionary using filename as key
                dataframes[filename[:-4]] = df

    _LOGGER.info("Dataframes created.")
    return dataframes
