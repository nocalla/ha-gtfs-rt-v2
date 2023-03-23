import io
import logging
import zipfile

import pandas as pd
import requests

_LOGGER = logging.getLogger(__name__)

# to be generalised using CONF values in normal use
GTFS_URL = (
    "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
)


class StaticMasterGTFSInfo:
    def __init__(self, url) -> None:
        # TODO - add a cache to this so it doesn't download constantly
        dataframes = get_dataframes(url)

        self.routes = dataframes["routes"]
        self.trips = dataframes["trips"]
        self.stop_times = dataframes["stop_times"]
        self.calendar = dataframes["calendar"]
        self.calendar_dates = dataframes["calendar_dates"]
        # self.shapes = dataframes["shapes"]

        # add agency data columns to routes data
        self.routes = pd.merge(self.routes, dataframes["agency"])
        _LOGGER.debug(self.routes.head())
        # merge stop details and stop schedule
        self.stops = pd.merge(self.stop_times, dataframes["stops"])
        _LOGGER.debug(self.stops.head())
        # fix timestamps
        self.stop_times["arrival_time"] = pd.to_datetime(
            self.stop_times["arrival_time"], format="%H:%M:%S"
        )


class RouteDetails:
    """
    Class for handling information in routes.txt,
    which provides details on the various routes provided by the operators.
    Plus information in agencies.txt.
    - Columns: route_id, agency_id, route_short_name, route_long_name,
    route_desc, route_type, route_url, route_color, route_text_color
    + agency_name, agency_url, agency_timezone (added from agencies.txt)

    - Look up relevant information using route_short_name - other parameters
    are variable.
    - route_short_name is the route number or Line Name - NB!
    - agency_id links to agency.txt
    - Currently no data in these fields: route_url route_color route_text_color
    """

    def __init__(self, identifier: str, df: pd.DataFrame) -> None:
        """
        Generate Route parameters from provided dataframe using the provided
        route_short_name as a search key.
        Saves all parameters as object parameters for external access.

        :param route_short_name: Route Number or Line Name, e.g. Green, 100X
        :type route_short_name: str
        :param routes_df: Dataframe sources from routes.txt with details of
        all routes
        :type routes_df: pd.DataFrame
        """
        self.trips = dict()
        self.short_name = identifier
        self.details = get_details_by_id(
            identifier=self.short_name,
            identifier_col="route_short_name",
            df=df,
        ).to_dict(orient="list")
        _LOGGER.debug(self.details)

        # this part feels clunky, but I'm staying explicit for now
        self.id = self.details["route_id"][0]
        self.agency_id = self.details["agency_id"][0]
        self.long_name = self.details["route_long_name"][0]
        self.desc = self.details["route_desc"][0]
        self.type = self.details["route_type"][0]
        self.url = self.details["route_url"][0]
        self.color = self.details["route_color"][0]
        self.text_color = self.details["route_text_color"][0]
        self.agency_name = self.details["agency_name"][0]
        self.agency_url = self.details["agency_url"][0]
        self.agency_timezone = self.details["agency_timezone"][0]


class TripInfo:
    """
    Class for handling information in trips.txt,
    which describes the trips, i.e. individual instances of all routes.
    Columns: route_id, service_id, trip_id, trip_headsign, trip_short_name,
    direction_id, block_id, shape_id

    - Look up relevant information using route_id
    - shape_id is lookup for shapes.txt
    """

    def __init__(self, identifier: str, df: pd.DataFrame) -> None:
        self.details = df
        self.id = identifier
        self.route_id = df["route_id"]
        self.service_id = df["service_id"]
        self.headsign = df["trip_headsign"]
        self.short_name = df["trip_short_name"]
        self.direction_id = df["direction_id"]
        self.block_id = df["block_id"]
        self.shape_id = df["shape_id"]
        _LOGGER.debug(self.details.head())


class StopSchedule:
    """
    Class for handling information from stops.txt,
    which provides the detail for all the stops referenced,
    and stop_times.txt,
    which provides details of all the stops on a particular trip
    and the expected arrival/departure times at each stop
    Columns: trip_id, arrival_time, departure_time, stop_id, stop_sequence,
    stop_headsign, pickup_type, drop_off_type, timepoint,
    stop_code, stop_name, stop_desc, stop_lat, stop_lon,
    zone_id, stop_url, location_type, parent_station

    - Look up the stop_id using stop_code, which corresponds to the
    stop's plate code
    - Look up using trip_id & stop_id to link between the
    TripInfo object and StopSchedule object
    - The stop_id field is a lookup reference to stops.txt
    - The field stop_headsign is populated with the first stop on the trip
    """

    def __init__(self, identifier: str, df: pd.DataFrame) -> None:
        self.details = (
            get_details_by_id(
                identifier=identifier,
                identifier_col="stop_code",
                df=df,
            )
            .set_index("trip_id")
            .to_dict(orient="index")
        )
        _LOGGER.debug(self.details)
        self.trip_ids = [str(k) for k in self.details.keys()]

        # self.arrival_time = stoptimes_details["arrival_time"][0]
        # self.departure_time = stoptimes_details["departure_time"][0]
        # self.stop_id = stoptimes_details["stop_id"][0]
        # self.stop_sequence = stoptimes_details["stop_sequence"][0]
        # self.stop_headsign = stoptimes_details["stop_headsign"][0]
        # self.pickup_type = stoptimes_details["pickup_type"][0]
        # self.drop_off_type = stoptimes_details["drop_off_type"][0]
        # self.timepoint = stoptimes_details["timepoint"][0]

    def get_trip_entry(self, trip_id: str):
        trip_entry = self.details[trip_id]
        trip_entry.update({"trip_id": trip_id})
        _LOGGER.debug(trip_entry)
        return trip_entry


class Calendar:  # TODO merge this into trip info

    """
    Class for handling information from calendar.txt,
    which determine what days a particular trip runs on, as well
    as the timeframe it runs in
    Columns: service_id, monday, tuesday, wednesday, thursday, friday,
    saturday, sunday, start_date, end_date

    - Look up relevant information using service_id
    """

    def __init__(self) -> None:
        raise NotImplementedError


class CalendarDates:  # TODO merge this into trip info?

    """
    Class for handling information from calendar_dates.txt,
    which lists exceptions to the service_ids in calendar.txt
    with an explicit date and an exception type, e.g. Bank Holidays etc
    Columns: service_id, date, exception_type

    - Look up relevant information using service_id in trips.txt
    """

    def __init__(self) -> None:
        raise NotImplementedError


class Shapes:  # TODO - decide what to do with these data

    """
    Class for handling information from shapes.txt,
    which lists the path that a vehicle travels along a route alignment
    Columns: shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence,
    shape_dist_traveled

    - Look up relevant information using service_id service_id = lookup to
    trips.txt
    - Look relevant information using shape_id in trips.txt
    """

    def __init__(self) -> None:
        raise NotImplementedError


class ScheduledDeparture:
    def __init__(
        self,
        Trip: TripInfo,
        Route: RouteDetails,
        Schedule: StopSchedule,
    ) -> None:
        # merge the info from the classes to generate a Departure class

        self.Trip = Trip
        self.Route = Route
        self.Schedule = Schedule

        schedule_details = Schedule.get_trip_entry(Trip.id)
        _LOGGER.debug(schedule_details)

        self.arrival_time = schedule_details["arrival_time"]
        self.departure_time = schedule_details["departure_time"]
        self.stop_id = schedule_details["stop_id"]
        self.stop_sequence = schedule_details["stop_sequence"]
        self.stop_headsign = schedule_details["stop_headsign"]
        self.pickup_type = schedule_details["pickup_type"]
        self.drop_off_type = schedule_details["drop_off_type"]
        self.timepoint = schedule_details["timepoint"]
        self.stop_code = schedule_details["stop_code"]
        self.stop_name = schedule_details["stop_name"]
        self.stop_desc = schedule_details["stop_desc"]
        self.stop_lat = schedule_details["stop_lat"]
        self.stop_lon = schedule_details["stop_lon"]
        self.zone_id = schedule_details["zone_id"]
        self.stop_url = schedule_details["stop_url"]
        self.location_type = schedule_details["location_type"]
        self.parent_station = schedule_details["parent_station"]
        self.trip_id = schedule_details["trip_id"]

    def string_departure_summary(self):
        return (
            f"Stop {self.stop_name} "
            f"({self.stop_code}), "
            f"Stop ID: {self.stop_id}, "
            f"Route: {self.Route.short_name} "
            f"({self.Route.long_name}), "
            f"Route ID: {self.Route.id}, "
            f"Trip ID: {self.trip_id}, "
            f"Arrival Time: {self.arrival_time}"
        )


def get_dataframes(url: str) -> dict[str, pd.DataFrame]:
    # Make a GET request to the URL
    _LOGGER.info("Requesting GTFS static data...")
    response = requests.get(url, timeout=10)
    _LOGGER.info(f"Request zip file successful {response.status_code}")

    # Load the zip file into a ZipFile object
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))

    # Define an empty dictionary to store the Pandas DataFrames
    dataframes = {}
    _LOGGER.info("Creating dataframes from source data...")
    # Loop through each file in the zip file
    for filename in zip_file.namelist():
        # Extract the file from the zip file
        if filename != "shapes.txt":
            _LOGGER.debug(f"Creating dataframe from {filename}...")
            with zip_file.open(filename, "r") as file:
                # Create a Pandas DataFrame from the file
                dataframe = pd.read_csv(
                    file,
                    dtype={"stop_code": str},
                )

                # Add the DataFrame to the dictionary using filename as key
                dataframes[filename[:-4]] = dataframe
    _LOGGER.info("Dataframes created.")
    return dataframes


def get_details_by_id(
    identifier: str, identifier_col: str, df: pd.DataFrame
) -> pd.DataFrame:
    _LOGGER.debug(
        f"Searching for {identifier_col} '{identifier}'\n{df.head()}\n"
    )  # DEBUG
    return df.loc[df[identifier_col] == identifier]


def get_stop_departures(
    MasterGTFSInfo: StaticMasterGTFSInfo, route: str, stop_code: str
) -> dict[str, ScheduledDeparture]:
    ThisStopSchedule = StopSchedule(
        identifier=stop_code, df=MasterGTFSInfo.stops
    )
    _LOGGER.debug(
        f"Getting departures for route {route} from stop {stop_code}..."
    )
    ThisRoute = RouteDetails(identifier=route, df=MasterGTFSInfo.routes)
    scheduled_trip_ids = ThisStopSchedule.trip_ids

    stop_departures = dict()
    for trip_id in scheduled_trip_ids:
        ThisTrip = TripInfo(identifier=trip_id, df=MasterGTFSInfo.trips)
        stop_departures.update(
            {
                trip_id: ScheduledDeparture(
                    Trip=ThisTrip,
                    Route=ThisRoute,
                    Schedule=ThisStopSchedule,
                )
            }
        )
    return stop_departures


# purely for testing
if __name__ == "__main__":
    TEST_ROUTE_NAME = "100X"
    TEST_STOP_CODE = "7399"

    MasterGTFSInfo = StaticMasterGTFSInfo(url=GTFS_URL)
    test_stop_departures = get_stop_departures(
        MasterGTFSInfo=MasterGTFSInfo,
        route=TEST_ROUTE_NAME,
        stop_code=TEST_STOP_CODE,
    )

    for i, trip in enumerate(test_stop_departures.keys()):
        self = test_stop_departures[trip]
        if i == 0:
            print(
                f"Stop: {self.stop_name}"
                f" ({self.stop_code}); "
                f" Stop ID: {self.stop_id}\n"
                f"Route: {self.Route.short_name} "
                f"({self.Route.long_name}),"
                f" Route ID: {self.Route.id}"
            )
        print(f"\t\t{self.Trip.id} - Scheduled arrival {self.arrival_time}")
