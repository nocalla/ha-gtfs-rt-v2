from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2
from homeassistant.util import Throttle
from StaticTimetable import GTFSCache, StaticMasterGTFSInfo
from utils import debug_dataframe, log_debug, log_error, log_info

MIN_TIME_BETWEEN_UPDATES = timedelta(seconds=60)


def get_gtfs_feed_entities(url: str, headers, label: str) -> list:
    """
    Returns a list of GTFS entities via an API endpoint.

    :param url: API endpoint
    :type url: str
    :param headers: Headers provided to endpoint.
    :type headers: _type_
    :param label: Label for logging purposes.
    :type label: str
    :return: List of GTFS entities from API.
    :rtype: list
    """
    feed = gtfs_realtime_pb2.FeedMessage()  # type: ignore
    response = requests.get(url, headers=headers, timeout=20)
    log_debug([f"Getting {label} info from API..."], 0)
    if response.status_code == 200:
        log_info([f"Successfully updated {label}", response.status_code], 0)
    else:
        log_error(
            [
                f"Updating {label} got",
                response.status_code,
                response.content,
            ],
            0,
        )

    feed.ParseFromString(response.content)
    return feed.entity


def gtfs_data_to_df(
    entities: list[gtfs_realtime_pb2.DESCRIPTOR],
    label: str,
) -> pd.DataFrame:
    """
    Convert a list of GTFS feed entities to a Pandas dataframe where the
    columns correspond to the desired data.

    :param entities: List of GTFS feed objects
    :type entities: list[gtfs_realtime_pb2.DESCRIPTOR]
    :param label: String denoting what type of data the request deals with.
    :type label: str
    :return: Pandas dataframe of GTFS data
    :rtype: pd.DataFrame
    """
    source_dict = defaultdict(list)
    for entity in entities:
        entity_id = entity.id

        if label == "trip":
            ThisTripData = entity.trip_update
            timestamp = ThisTripData.timestamp
            ThisTrip = ThisTripData.trip
            trip_id = ThisTrip.trip_id
            route_id = ThisTrip.route_id
            # format start date
            # increment by 1 day because of API bug where the start date is
            # always 1 day behind
            start_date_dt = datetime.strptime(
                ThisTrip.start_date,
                "%Y%m%d",
            ) + timedelta(days=1)
            start_time_dt = ThisTrip.start_time
            # convert start date and time to Unix time
            start_date = start_date_dt.timestamp()
            start_time = (
                pd.to_timedelta(start_time_dt).to_pytimedelta().total_seconds()
            )

            schedule_relationship = ThisTrip.schedule_relationship
            direction_id = ThisTrip.direction_id
            vehicle_id = ThisTripData.vehicle.id

            for stop in ThisTripData.stop_time_update:
                # Overall entity info
                source_dict["trip_entity_id"].append(entity_id)
                source_dict["timestamp"].append(timestamp)
                source_dict["real_time_update"].append(True)
                # Trip-specific Information
                source_dict["trip_id"].append(trip_id)
                source_dict["route_id"].append(route_id)
                source_dict["start_time_dt"].append(start_time_dt)
                source_dict["start_date_str"].append(ThisTrip.start_date)
                source_dict["start_date_dt"].append(start_date_dt)
                source_dict["start_time"].append(start_time)
                source_dict["start_date"].append(start_date)
                source_dict["schedule_relationship"].append(
                    schedule_relationship
                )
                source_dict["direction_id"].append(direction_id)
                source_dict["vehicle_id"].append(vehicle_id)
                # Stop Information
                source_dict["stop_id"].append(stop.stop_id)
                source_dict["stop_sequence"].append(stop.stop_sequence)
                source_dict["live_arrival_time"].append(stop.arrival.time)
                source_dict["arrival_delay"].append(stop.arrival.delay)
                source_dict["live_departure_time"].append(stop.departure.time)
                source_dict["departure_delay"].append(stop.departure.delay)

        if label == "vehicle":
            vehicle = entity.vehicle
            ThisTrip = vehicle.trip

            trip_id = ThisTrip.trip_id
            vehicle_id = vehicle.vehicle.id

            if not vehicle.trip.trip_id:
                # Vehicle is not in service
                continue
            # Overall entity info
            source_dict["vehicle_entity_id"].append(entity_id)
            # trip-specific
            source_dict["trip_id"].append(trip_id)
            # vehicle-specific
            source_dict["vehicle_id"].append(vehicle_id)
            source_dict["vehicle_latitude"].append(vehicle.position.latitude)
            source_dict["vehicle_longitude"].append(vehicle.position.longitude)

    df = pd.DataFrame(source_dict)
    if label == "trip":
        df["direction_id"] = df["direction_id"].astype("category")
        df["schedule_relationship"] = df["schedule_relationship"].astype(
            "category"
        )

    # convert all object types to categories
    df[df.select_dtypes(["object"]).columns] = df.select_dtypes(
        ["object"]
    ).apply(lambda x: x.astype("category"))

    return df


class PublicTransportData:
    """The Class for handling API data retrieval."""

    def __init__(
        self,
        trip_update_url: str,
        static_gtfs_url: str = "",
        vehicle_position_url: str = "",
        route_delimiter: str = "",
        api_key: str = "",
        x_api_key: str = "",
    ):
        """Initialize the info object."""
        self._trip_update_url = trip_update_url
        self.static_gtfs_url = static_gtfs_url
        self._vehicle_position_url = vehicle_position_url
        self._route_delimiter = route_delimiter
        if api_key is not None:
            self._headers = {"Authorisation": api_key}
        elif x_api_key is not None:
            self._headers = {"x-api-key": x_api_key}
        else:
            self._headers = None
        self.info_df = pd.DataFrame()
        self.CachedGTFSData = GTFSCache()

    def get_live_data(self) -> pd.DataFrame:
        # create trip update dataframe
        # Use list comprehension to filter entities with trip_update field
        feed_entities = [
            entity
            for entity in get_gtfs_feed_entities(
                url=self._trip_update_url,
                headers=self._headers,
                label="trip data",
            )
            if entity.HasField("trip_update")
        ]

        trip_update_df = gtfs_data_to_df(feed_entities, label="trip")
        debug_dataframe(trip_update_df, "Trip Update")

        # create vehicle info dataframe
        v_feed_entities = get_gtfs_feed_entities(
            url=self._vehicle_position_url,
            headers=self._headers,
            label="vehicle positions",
        )
        vehicle_info_df = gtfs_data_to_df(v_feed_entities, label="vehicle")
        debug_dataframe(vehicle_info_df, "Vehicle Info")

        trip_update_df = pd.merge(
            left=trip_update_df, right=vehicle_info_df, how="left"
        )
        debug_dataframe(trip_update_df, "Merged Trip & Vehicle Info")
        return trip_update_df

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    def update(self) -> None:
        log_info(
            [
                "trip_update_url",
                self._trip_update_url,
                "\nvehicle_position_url",
                self._vehicle_position_url,
                "\nheader",
                self._headers,
            ],
            0,
        )
        # get static timetable data
        timetable_df = StaticMasterGTFSInfo(
            url=self.static_gtfs_url, CachedData=self.CachedGTFSData
        ).departure_info
        debug_dataframe(timetable_df, "Static Timetable Info")

        live_df = self.get_live_data()
        # merge live and static data
        trip_update_df = pd.merge(
            live_df,
            timetable_df,
            how="outer",
            on="trip_id",
            indicator="source",
        )

        # duplicated columns fixing
        duplicated_columns = [
            "stop_id",
            "route_id",
            "direction_id",
            "stop_sequence",
        ]

        # If column from X is NA, fill with value from Y
        for column in duplicated_columns:
            trip_update_df[column] = (
                trip_update_df[f"{column}_x"]
                .astype(str)
                .fillna(trip_update_df[f"{column}_y"].astype(str))
            )
        # drop intermediate columns
        trip_update_df = trip_update_df.drop(
            columns=[f"{c}_x" for c in duplicated_columns]
            + [f"{c}_y" for c in duplicated_columns]
        )
        debug_dataframe(trip_update_df, "Merged live and static info")

        # need to split route_ids if there's a delimiter
        if self._route_delimiter is not None:
            trip_update_df["route_id", "route_id_split"] = trip_update_df[
                "route_id"
            ].str.split(self._route_delimiter, expand=True)

        # if no start_date specified, set it to today
        # if arrival time is in the past, set start_date to tomorrow
        now = datetime.today()
        now_secs = now.timestamp()
        midnight = datetime.combine(now, datetime.min.time())
        secs_since_midnight = (now - midnight).total_seconds()
        midnight_tomorrow = midnight + timedelta(days=1)

        # set start_date to today for all rows missing start dates
        trip_update_df["start_date"] = trip_update_df["start_date"].fillna(
            midnight.timestamp()
        )
        trip_update_df["start_date_dt"] = trip_update_df[
            "start_date_dt"
        ].fillna(midnight)
        debug_dataframe(trip_update_df, "Fill in missing times")  # debug

        # do some maths on the dataframe
        # if no arrival_delay specified, set to 0
        trip_update_df["arrival_delay"] = trip_update_df[
            "arrival_delay"
        ].fillna(0)

        # work out updated_arrival_time
        # updated_arrival_time = arrival_time + start_date + arrival_delay
        # OR live_arrival_time if not zero
        # set value as 0 if no valid time produced

        trip_update_df["updated_arrival_time"] = (
            trip_update_df["live_arrival_time"]
            .mask(lambda x: x == 0)  # type: ignore
            .fillna(
                trip_update_df["scheduled_arrival_time"]
                + trip_update_df["start_date"]
                + trip_update_df["arrival_delay"]
            )
        ).fillna(0)

        # work out updated_departure_time
        trip_update_df["updated_departure_time"] = (
            trip_update_df["live_departure_time"]
            .mask(lambda x: x == 0)  # type: ignore
            .fillna(
                trip_update_df["scheduled_departure_time"]
                + trip_update_df["start_date"]
                + trip_update_df["arrival_delay"]
            )
        ).fillna(0)

        # if updated_arrival_time is in the past, add the service to tomorrow
        # using the scheduled time (drop the delay)
        tomorrow_mask = (
            trip_update_df["updated_arrival_time"] < secs_since_midnight
        )
        tomorrow_services = trip_update_df.loc[tomorrow_mask].copy()
        tomorrow_services["start_date"] = midnight_tomorrow.timestamp()
        tomorrow_services["start_date_dt"] = midnight_tomorrow
        tomorrow_services["updated_arrival_time"] = (
            midnight_tomorrow.timestamp()
            + tomorrow_services["scheduled_arrival_time"]
        )
        # update original dataframe with tomorrow's services
        trip_update_df.update(tomorrow_services)

        debug_dataframe(trip_update_df, "Stop Time Calculation")

        # remove rows where updated_arrival_time is in the past
        log_debug(["Removing times in the past..."], 0)

        trip_update_df = trip_update_df.query(
            f"updated_arrival_time - {now_secs} >=0"
        )
        debug_dataframe(trip_update_df, "Filter out past Stop Times")

        # filter out days when the service doesn't run & exceptions
        # TODO!

        # fill in empty real_time_update values
        trip_update_df.loc[
            trip_update_df["real_time_update"].isnull(), "real_time_update"
        ] = False
        trip_update_df.loc[
            trip_update_df["vehicle_latitude"].isnull(), "vehicle_latitude"
        ] = "-"
        trip_update_df.loc[
            trip_update_df["vehicle_longitude"].isnull(), "vehicle_longitude"
        ] = "-"

        # return completed dataframe
        self.info_df = trip_update_df

    def filter_df(
        self,
        filters: dict,
        order_by: str,
        order_ascending: bool,
        limit: int = 30,
    ) -> dict:
        """
        Filter a dataframe using the provided filters and return a numbered
        dictionary where each top-level entry corresponds to a service.
        The dictionary top level is in order of the column specified by the
        "order_by" parameter.

        :param filters: Dictionary mapping column name to desired value,
        can be any number of columns.
        :type filters: dict
        :param order_by: Column to sort dataframe with.
        :type order_by: str
        :param order_ascending: Whether to sort ascending or descending.
        :type order_ascending: bool
        :param limit: Number of results to limit the filter result to
        :type limit: int
        :return: Nested dictionary in the following form representing the
        original dataframe after filters have been applied.
        {0:{col1:x, col2:y...}, 1:{col1:x, col2:y...}...}
        The top level represents the order of the dataframe rows as defined by
        the "order_by" parameter.
        :rtype: dict
        """
        log_debug([f"Filters: {filters}"], 0)
        query_str = " and ".join(
            [
                f"{key}.astype('{type(value).__name__}') == {value!r}"
                if pd.api.types.is_categorical_dtype(self.info_df[key])
                else f"{key} == {value!r}"
                if isinstance(value, str)
                else f"{key} == {value}"
                for key, value in filters.items()
                if value is not None or ""
            ]
        )
        log_debug([f"Filtering data using filter query ({query_str})..."], 0)

        filtered_df = (
            self.info_df.query(query_str)
            .sort_values(by=[order_by], ascending=order_ascending)
            .reset_index()
        )
        debug_dataframe(filtered_df, "Filtered data")
        return filtered_df.head(limit).to_dict(orient="index")
