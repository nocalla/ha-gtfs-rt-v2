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
        self.stops = dataframes["stops"]
        self.stop_times = dataframes["stop_times"]
        self.calendar = dataframes["calendar"]
        self.calendar_dates = dataframes["calendar_dates"]
        self.shapes = dataframes["shapes"]

        # add agency data columns to routes data
        self.routes = pd.merge(self.routes, dataframes["agency"])


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

    def __init__(self, route_short_name: str, routes_df: pd.DataFrame) -> None:
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
        self.route_short_name = route_short_name
        self.route_details = get_details_by_id(
            identifier=self.route_short_name,
            identifier_col="route_short_name",
            df=routes_df,
        ).to_dict(orient="list")

        # this part feels clunky, but I'm staying explicit for now
        self.route_id = self.route_details["route_id"][0]
        self.agency_id = self.route_details["agency_id"][0]
        self.route_long_name = self.route_details["route_long_name"][0]
        self.route_desc = self.route_details["route_desc"][0]
        self.route_type = self.route_details["route_type"][0]
        self.route_url = self.route_details["route_url"][0]
        self.route_color = self.route_details["route_color"][0]
        self.route_text_color = self.route_details["route_text_color"][0]
        self.agency_name = self.route_details["agency_name"][0]
        self.agency_url = self.route_details["agency_url"][0]
        self.agency_timezone = self.route_details["agency_timezone"][0]

    def get_trip_dict(self, trips_df: pd.DataFrame) -> dict:
        return (
            get_details_by_id(
                identifier=self.route_id,
                identifier_col="route_id",
                df=trips_df,
            )
            .set_index("trip_id")
            .to_dict(orient="index")
        )


class TripInfo:
    """
    Class for handling information in trips.txt,
    which describes the trips, i.e. individual instances of all routes.
    Columns: route_id, service_id, trip_id, trip_headsign, trip_short_name,
    direction_id, block_id, shape_id

    - Look up relevant information using route_id
    - shape_id is lookup for shapes.txt
    """

    def __init__(self, trip_id: str, trip_details: dict) -> None:
        # trip_details = get_details_by_id(
        #     identifier=trip_id,
        #     identifier_col="trip_id",
        #     df=trips_df,
        # ).to_dict(orient="list")

        self.trip_id = trip_id
        self.route_id = trip_details["route_id"]
        self.service_id = trip_details["service_id"]
        self.trip_headsign = trip_details["trip_headsign"]
        self.trip_short_name = trip_details["trip_short_name"]
        self.direction_id = trip_details["direction_id"]
        self.block_id = trip_details["block_id"]
        self.shape_id = trip_details["shape_id"]


class StopDetails:
    """
    Class for handling information in stops.txt,
    which provides the detail for all the stops referenced.
    Columns: stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon,
    zone_id, stop_url, location_type, parent_station

    - Look up relevant information using stop_code, which corresponds to the
    stop's plate code
    """

    def __init__(self, stop_code: str, stop_df: pd.DataFrame) -> None:
        stop_details = get_details_by_id(
            identifier=stop_code,
            identifier_col="stop_code",
            df=stop_df,
        ).to_dict(orient="list")

        self.stop_code = stop_code
        self.stop_id = stop_details["stop_id"]
        self.stop_name = stop_details["stop_name"]
        self.stop_desc = stop_details["stop_desc"]
        self.stop_lat = stop_details["stop_lat"]
        self.stop_lon = stop_details["stop_lon"]
        self.zone_id = stop_details["zone_id"]
        self.stop_url = stop_details["stop_url"]
        self.location_type = stop_details["location_type"]
        self.parent_station = stop_details["parent_station"]


class StopTimes:
    """
    Class for handling information from stop_times.txt,
    which provides details of all the stops on a particular trip
    and the expected arrival/departure times at each stop
    Columns: trip_id, arrival_time, departure_time, stop_id, stop_sequence,
    stop_headsign, pickup_type, drop_off_type, timepoint

    - Look up relevant information using trip_id & stop_id to link between the
    TripInfo object and StopDetails object
    - The stop_id field is a lookup reference to stops.txt
    - The field stop_headsign is populated with the first stop on the trip
    """

    def __init__(self, stop_id: str, stoptimes_df: pd.DataFrame) -> None:
        stoptimes_details = get_details_by_id(
            identifier=stop_id,
            identifier_col="stop_id",
            df=stoptimes_df,
        ).to_dict(orient="list")

        print(stoptimes_details)

        # self.trip_id = stoptimes_details["trip_id"][0]
        # self.arrival_time = stoptimes_details["arrival_time"][0]
        # self.departure_time = stoptimes_details["departure_time"][0]
        # self.stop_id = stoptimes_details["stop_id"][0]
        # self.stop_sequence = stoptimes_details["stop_sequence"][0]
        # self.stop_headsign = stoptimes_details["stop_headsign"][0]
        # self.pickup_type = stoptimes_details["pickup_type"][0]
        # self.drop_off_type = stoptimes_details["drop_off_type"][0]
        # self.timepoint = stoptimes_details["timepoint"][0]

    def get_trip_entry(self, trip_id: str):
        pass


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
        ThisTrip: TripInfo,
        ThisStop: StopDetails,
        ThisRoute: RouteDetails,
        ThisSchedule: StopTimes,
    ) -> None:
        # merge the info from the two classes to generate a Departure class
        print(
            f"{ThisTrip.trip_id} {ThisRoute.route_long_name}"
            f"{ThisStop.stop_name}"
        )

    def display_departure_info(self) -> str:
        dep_info = str()
        return dep_info


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
            dataframe = pd.read_csv(file)

            # Add the DataFrame to the dictionary using filename as key
            dataframes[filename[:-4]] = dataframe
    return dataframes


def get_details_by_id(
    identifier: str, identifier_col: str, df: pd.DataFrame
) -> pd.DataFrame:
    return df.loc[df[identifier_col] == identifier]


def get_stop_departures(
    url: str, route: str, stop_code: str
) -> dict[str, Departure]:
    MasterInfo = StaticMasterInfo(url=url)
    ThisRoute = RouteDetails(
        route_short_name=route, routes_df=MasterInfo.routes
    )
    ThisStop = StopDetails(stop_code=stop_code, stop_df=MasterInfo.stops)
    this_route_trips = ThisRoute.get_trip_dict(MasterInfo.trips)
    ThisStopSchedule = StopTimes(
        stop_id=ThisStop.stop_id, stoptimes_df=MasterInfo.stop_times
    )

    departure_dict = dict()
    for trip_id in this_route_trips:
        ThisTrip = TripInfo(
            trip_id=trip_id, trip_details=this_route_trips[trip_id]
        )
        ThisSchedule = ThisStopSchedule.get_trip_entry(trip_id)
        departure_dict.update(
            {
                trip_id: Departure(
                    ThisTrip, ThisStop, ThisRoute, ThisSchedule=ThisSchedule
                )
            }
        )
    return departure_dict


# purely for testing
if __name__ == "__main__":
    TEST_ROUTE_NAME = "100X"
    TEST_STOP_CODE = "7399"

    test_stop_departures = get_stop_departures(
        url=GTFS_URL, route=TEST_ROUTE_NAME, stop_code=TEST_STOP_CODE
    )
    for trip in test_stop_departures.keys():
        test_stop_departures[trip].display_departure_info()
