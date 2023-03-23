import io
import zipfile

import pandas as pd
import requests
from sensor import _LOGGER

# to be generalised using CONF values in normal use
GTFS_URL = (
    "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
)


class StaticMasterInfo:
    def __init__(self, url) -> None:
        # TODO - add a cache to this so it doesn't download constantly
        dataframes = get_dataframes(url)

        self.routes = dataframes["routes"]
        self.trips = dataframes["trips"]
        self.stop_times = dataframes["stop_times"]
        self.calendar = dataframes["calendar"]
        self.calendar_dates = dataframes["calendar_dates"]
        self.shapes = dataframes["shapes"]

        # add agency data columns to routes data
        self.routes = pd.merge(self.routes, dataframes["agency"])
        # merge stop details and stop schedule
        self.stops = pd.merge(self.stop_times, dataframes["stops"])


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


class Departure:
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

        self.trip_id = Trip.id
        self.route_long_name = Route.long_name
        self.schedule_details = Schedule.get_trip_entry(Trip.id)

        self.arrival_time = self.schedule_details["arrival_time"]
        self.departure_time = self.schedule_details["departure_time"]
        self.stop_id = self.schedule_details["stop_id"]
        self.stop_sequence = self.schedule_details["stop_sequence"]
        self.stop_headsign = self.schedule_details["stop_headsign"]
        self.pickup_type = self.schedule_details["pickup_type"]
        self.drop_off_type = self.schedule_details["drop_off_type"]
        self.timepoint = self.schedule_details["timepoint"]
        self.stop_code = self.schedule_details["stop_code"]
        self.stop_name = self.schedule_details["stop_name"]
        self.stop_desc = self.schedule_details["stop_desc"]
        self.stop_lat = self.schedule_details["stop_lat"]
        self.stop_lon = self.schedule_details["stop_lon"]
        self.zone_id = self.schedule_details["zone_id"]
        self.stop_url = self.schedule_details["stop_url"]
        self.location_type = self.schedule_details["location_type"]
        self.parent_station = self.schedule_details["parent_station"]
        self.trip_id = self.schedule_details["trip_id"]


def get_dataframes(url: str) -> dict[str, pd.DataFrame]:
    # Make a GET request to the URL
    response = requests.get(url, timeout=10)
    _LOGGER.info(f"Request zip file successful {response.status_code}")

    # Load the zip file into a ZipFile object
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))

    # Define an empty dictionary to store the Pandas DataFrames
    dataframes = {}

    # Loop through each file in the zip file
    for filename in zip_file.namelist():
        # Extract the file from the zip file
        with zip_file.open(filename, "r") as file:
            # Create a Pandas DataFrame from the file
            dataframe = pd.read_csv(file, dtype={"stop_code": str})

            # Add the DataFrame to the dictionary using filename as key
            dataframes[filename[:-4]] = dataframe
    return dataframes


def get_details_by_id(
    identifier: str, identifier_col: str, df: pd.DataFrame
) -> pd.DataFrame:
    # print(f"Searching for {identifier_col}: {identifier}"f
    # "\n{df.head()}\n") # DEBUG
    return df.loc[df[identifier_col] == identifier]


def get_stop_departures(
    url: str, route: str, stop_code: str
) -> dict[str, Departure]:
    MasterInfo = StaticMasterInfo(url=url)

    ThisStopSchedule = StopSchedule(identifier=stop_code, df=MasterInfo.stops)
    ThisRoute = RouteDetails(identifier=route, df=MasterInfo.routes)
    scheduled_trip_ids = ThisStopSchedule.trip_ids

    stop_departures = dict()
    for trip_id in scheduled_trip_ids:
        ThisTrip = TripInfo(identifier=trip_id, df=MasterInfo.trips)
        stop_departures.update(
            {
                trip_id: Departure(
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

    test_stop_departures = get_stop_departures(
        url=GTFS_URL, route=TEST_ROUTE_NAME, stop_code=TEST_STOP_CODE
    )

    for i, trip in enumerate(test_stop_departures.keys()):
        TestDeparture = test_stop_departures[trip]
        if i == 0:
            print(
                f"Stop: {TestDeparture.stop_name}"
                f" ({TestDeparture.stop_code}); "
                f"Route: {TestDeparture.Route.short_name} "
                f"({TestDeparture.Route.long_name})"
            )
        print(
            f"\t\t{TestDeparture.Trip.id} - Scheduled arrival"
            f" {TestDeparture.arrival_time}"
        )
