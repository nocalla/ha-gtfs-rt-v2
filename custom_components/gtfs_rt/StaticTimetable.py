# agency.txt = transport operators for the routes
#   agency_id agency_name agency_url agency_timezone

# routes.txt = describe the various routes provided by the operators
#       route_id agency_id route_short_name route_long_name route_desc
#       route_type route_url route_color route_text_color
#
#       note route_id will not be persistent - will generate on schedule upload
#       agency_id links to agency.txt
#       route_short_name is the route number or Line Name - NB!
#       no data in these fields: route_url route_color route_text_color

# trips.txt = describes the trips – a trip being a single instance of a route
#       route_id service_id trip_id trip_headsign trip_short_name direction_id
#       block_id shape_id
#
#       route_id = lookup to routes.txt, shape_id is lookup to shapes.txt

# stops.txt = provides the detail for all the stops referenced
#       stop_id stop_code stop_name stop_desc stop_lat stop_lon zone_id
#       stop_url location_type parent_station

# stop_times.txt = gives details of all the stops on a particular trip
# and the expected arrival/departure times at each stop
#       trip_id arrival_time departure_time stop_id stop_sequence
#       stop_headsign pickup_type drop_off_type timepoint
#
#       trip_id remains the lookup reference to trips.txt
#       The stop_id field is a lookup reference to stops.txt
#       The field stop_headsign is populated with the first stop on the trip


# calendar.txt = determine what days a particular trip runs on, as well
# as the timeframe it runs in
#       service_id Monday Tuesday Wednesday Thursday Friday saturday Sunday
#       start_date end_date
#
#       service_id remains the lookup reference to service_id in trips.txt

# calendar_dates.txt = list of exceptions to the service_ids in calendars.txt
# with an explicit date and an exception type, e.g. Bank Holidays etc
#       service_id date exception_type
#       service_id = lookup to trips.txt

#  shapes.txt = the path that a vehicle travels along a route alignment
#       shape_id shape_pt_lat shape_pt_lon shape_pt_sequence
#       shape_dist_traveled
#
#       shape_id = lookup reference to shape_id in trips.txt

import io
import zipfile

import pandas as pd
import requests

# to be generalised using CONF values in normal use
GTFS_URL = (
    "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
)


class StaticTimetable:
    def __init__(self, url) -> None:
        self.dataframes = get_dataframes(url)


class RouteDetails:
    def __init__(
        self, route_short_name: str, timetable_dfs: dict[str, pd.DataFrame]
    ) -> None:
        self.route_short_name = route_short_name
        self.route_details = get_route_details(
            self.route_short_name, timetable_dfs["routes"]
        )
        self.route_id = self.route_details.iloc[0]["route_id"]
        self.agency_id = self.route_details.iloc[0]["agency_id"]
        self.route_long_name = self.route_details.iloc[0]["route_long_name"]
        self.route_desc = self.route_details.iloc[0]["route_desc"]
        self.route_type = self.route_details.iloc[0]["route_type"]
        self.route_url = self.route_details.iloc[0]["route_url"]
        self.route_color = self.route_details.iloc[0]["route_color"]
        self.route_text_color = self.route_details.iloc[0]["route_text_color"]


class TripDetails:
    def __init__(self) -> None:
        pass


class StopDetails:
    def __init__(self) -> None:
        pass


def get_dataframes(url: str) -> dict[str, pd.DataFrame]:
    # Make a GET request to the URL
    response = requests.get(url, timeout=10)
    print(f"Request zip file successful {response.status_code}")

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
            print(f"\n{filename[:-4]}\n{dataframe.head()}\n{dataframe.shape}")
    return dataframes


# this can probably be generalised across all the data sources
def get_route_details(route_short_name: str, routes_df: pd.DataFrame):
    return routes_df.loc[routes_df.route_short_name == route_short_name]


# purely for testing
if __name__ == "__main__":
    timetable_dfs = StaticTimetable(url=GTFS_URL).dataframes
    test_route = RouteDetails(
        route_short_name="100X", timetable_dfs=timetable_dfs
    )
    print(f"{test_route.route_short_name}:\n{test_route.route_long_name}")
