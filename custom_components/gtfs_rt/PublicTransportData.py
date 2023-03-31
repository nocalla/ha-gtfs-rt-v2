from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2
from homeassistant.util import Throttle
from StaticTimetable import GTFSCache, StaticMasterGTFSInfo
from utils import debug_dataframe, log_debug, log_error, log_info

MIN_TIME_BETWEEN_UPDATES = timedelta(seconds=60)


def get_gtfs_feed_entities(url: str, headers, label: str):
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


def gtfs_tripupdate_to_df(
    entities: list[gtfs_realtime_pb2.DESCRIPTOR],
) -> pd.DataFrame:
    """
    Convert a list of GTFS feed entities to a Pandas dataframe where the
    columns correspond to the Trip data.
    :param entities: List of GTFS feed objects
    :type entities: list[gtfs_realtime_pb2.DESCRIPTOR]
    :return: Pandas dataframe of GTFS trip data
    :rtype: pd.DataFrame
    """
    source_dict = defaultdict(list)
    for entity in entities:
        entity_id = entity.id
        ThisTripData = entity.trip_update
        ThisTrip = ThisTripData.trip
        trip_id = ThisTrip.trip_id
        route_id = ThisTrip.route_id

        start_date = datetime.strptime(
            ThisTrip.start_date,
            "%Y%m%d",
        )
        start_time = pd.to_timedelta(ThisTrip.start_time)

        schedule_relationship = ThisTrip.schedule_relationship
        direction_id = ThisTrip.direction_id
        vehicle_id = ThisTripData.vehicle.id

        for stop in ThisTripData.stop_time_update:
            # Overall entity info
            source_dict["trip_entity_id"].append(entity_id)
            # Trip-specific Information
            source_dict["trip_id"].append(trip_id)
            source_dict["route_id"].append(route_id)
            source_dict["start_time"].append(start_time)
            source_dict["start_date"].append(start_date)
            source_dict["schedule_relationship"].append(schedule_relationship)
            source_dict["direction_id"].append(direction_id)
            source_dict["vehicle_id"].append(vehicle_id)
            # Stop Information
            source_dict["stop_id"].append(stop.stop_id)
            source_dict["stop_sequence"].append(stop.stop_sequence)
            source_dict["live_arrival_time"].append(stop.arrival.time)
            source_dict["arrival_delay"].append(stop.arrival.delay)
    df = pd.DataFrame(source_dict)

    # convert all object types to categories
    df[df.select_dtypes(["object"]).columns] = df.select_dtypes(
        ["object"]
    ).apply(lambda x: x.astype("category"))
    df["direction_id"] = df["direction_id"].astype("bool")
    df["schedule_relationship"] = df["schedule_relationship"].astype("bool")

    return df


def gtfs_vehicleinfo_to_df(
    entities: list[gtfs_realtime_pb2.DESCRIPTOR],
) -> pd.DataFrame:
    """
    Convert a list of GTFS feed entities to a Pandas dataframe where the
    columns correspond to the Vehicle data.
    :param entities: List of GTFS feed objects
    :type entities: list[gtfs_realtime_pb2.DESCRIPTOR]
    :return: Pandas dataframe of GTFS vehicle data
    :rtype: pd.DataFrame
    """
    source_dict = defaultdict(list)
    for entity in entities:
        entity_id = entity.id
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

    # convert all object types to categories
    df = pd.DataFrame(source_dict)
    df[df.select_dtypes(["object"]).columns] = df.select_dtypes(
        ["object"]
    ).apply(lambda x: x.astype("category"))

    return df


class PublicTransportData:
    """The Class for handling the data retrieval."""

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
            self._headers = {"Authorization": api_key}
        elif x_api_key is not None:
            self._headers = {"x-api-key": x_api_key}
        else:
            self._headers = None
        self.info_df = dict()
        self.CachedGTFSData = GTFSCache()

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    def update(self) -> None:
        log_info(
            [
                "trip_update_url",
                self._trip_update_url,
                "\nvehicle_position_url",
                self._vehicle_position_url,
                "\nroute_delimiter",
                self._route_delimiter,
                "\nheader",
                self._headers,
            ],
            0,
        )

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

        # make our dataframes!
        trip_update_df = gtfs_tripupdate_to_df(feed_entities)
        log_debug(
            [debug_dataframe(trip_update_df, "Trip Update")],
            0,
        )

        v_feed_entities = get_gtfs_feed_entities(
            url=self._vehicle_position_url,
            headers=self._headers,
            label="vehicle positions",
        )

        vehicle_info_df = gtfs_vehicleinfo_to_df(v_feed_entities)
        log_debug(
            [debug_dataframe(vehicle_info_df, "Vehicle Info")],
            0,
        )

        # add vehicle to trip update info TODO: fix merging- some missing?
        trip_update_df = pd.merge(
            left=trip_update_df, right=vehicle_info_df, how="left"
        )
        log_debug(
            [debug_dataframe(trip_update_df, "Merged Trip & Vehicle Info")],
            0,
        )
        # get static timetable data
        timetable_df = StaticMasterGTFSInfo(
            url=self.static_gtfs_url, CachedData=self.CachedGTFSData
        ).departure_info

        log_debug(
            [debug_dataframe(timetable_df, "Static Timetable Info")],
            0,
        )
        # merge live and static data
        trip_update_df = pd.merge(
            trip_update_df, timetable_df, how="left", indicator="source"
        )
        log_debug(
            [debug_dataframe(trip_update_df, "Merged live and static info")],
            0,
        )

        # do some maths on the dataframe
        # work out stop_time
        # stop_time = arrival_time + start_date + arrival_delay
        # OR live_arrival_time if not zero
        trip_update_df["stop_time"] = trip_update_df.apply(
            lambda row: pd.to_datetime(
                row["live_arrival_time"], unit="s", utc=True
            )
            if row["live_arrival_time"] != 0
            else pd.to_datetime(
                row["arrival_time"]
                + row["start_date"]
                + pd.to_timedelta(row["arrival_delay"], unit="s"),
                utc=True,
            ),
            axis=1,
        )
        log_debug(
            [debug_dataframe(trip_update_df, "Stop Time Calculation")],
            0,
        )
        # remove rows where stop_time is in the past

        now = pd.to_datetime("now", utc=True)
        log_debug([f"Removing times before {now}..."], 0)
        trip_update_df = trip_update_df[(trip_update_df["stop_time"] >= now)]
        log_debug(
            [debug_dataframe(trip_update_df, "Filter out past Stop Times")],
            0,
        )
        # return completed dataframe
        self.info_df = trip_update_df
