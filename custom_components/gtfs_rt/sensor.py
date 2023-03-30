import logging
from collections import defaultdict
from datetime import datetime, timedelta

import homeassistant.helpers.config_validation as cv
import homeassistant.util.dt as dt_util
import pandas as pd
import requests
import voluptuous as vol
from google.transit import gtfs_realtime_pb2
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import ATTR_LATITUDE, ATTR_LONGITUDE, CONF_NAME
from homeassistant.helpers.entity import Entity
from homeassistant.util import Throttle
from StaticTimetable import GTFSCache, StaticMasterGTFSInfo

_LOGGER = logging.getLogger(__name__)

ATTR_STOP_ID = "Stop ID"
ATTR_ROUTE = "Route"
ATTR_DIRECTION_ID = "Direction ID"
ATTR_DUE_IN = "Due in"
ATTR_DUE_AT = "Due at"
ATTR_NEXT_UP = "Next Service"
ATTR_ICON = "Icon"

CONF_API_KEY = "api_key"
CONF_X_API_KEY = "x_api_key"
CONF_STOP_ID = "stopid"
CONF_ROUTE = "route"
CONF_DIRECTION_ID = "directionid"
CONF_DEPARTURES = "departures"
CONF_TRIP_UPDATE_URL = "trip_update_url"
CONF_VEHICLE_POSITION_URL = "vehicle_position_url"
CONF_ROUTE_DELIMITER = "route_delimiter"
CONF_ICON = "icon"
CONF_SERVICE_TYPE = "service_type"

# these parameters are new for static GTFS integration
CONF_GTFS_URL = "gtfs_url"
CONF_ROUTE_LOOKUP = "route_name"
CONF_STOP_CODE = "stop_code"
# end of new parameters

DEFAULT_SERVICE = "Service"
DEFAULT_ICON = "mdi:bus"
DEFAULT_DIRECTION = 1

MIN_TIME_BETWEEN_UPDATES = timedelta(seconds=60)
TIME_STR_FORMAT = "%H:%M"

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_TRIP_UPDATE_URL): cv.string,
        vol.Optional(CONF_API_KEY): cv.string,
        vol.Optional(CONF_X_API_KEY): cv.string,
        vol.Optional(CONF_VEHICLE_POSITION_URL): cv.string,
        vol.Optional(CONF_ROUTE_DELIMITER): cv.string,
        vol.Required(CONF_GTFS_URL): cv.string,
        vol.Optional(CONF_DEPARTURES): [
            {
                vol.Required(CONF_NAME): cv.string,
                vol.Required(CONF_STOP_ID): cv.string,
                vol.Required(CONF_ROUTE): cv.string,
                vol.Optional(
                    CONF_DIRECTION_ID,
                    default=DEFAULT_DIRECTION,  # type: ignore
                ): bool,
                vol.Optional(
                    CONF_ICON, default=DEFAULT_ICON  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_SERVICE_TYPE, default=DEFAULT_SERVICE  # type: ignore
                ): cv.string,
                vol.Required(CONF_ROUTE_LOOKUP): cv.string,
                vol.Required(CONF_STOP_CODE): cv.string,
            }
        ],
    }
)


def due_in_mins(time: datetime):
    """Get the remaining minutes from now until a given datetime object."""
    diff = time - dt_util.now().replace(tzinfo=None)
    return int(diff.total_seconds() / 60)


def log_info(data: list, indent_level: int) -> None:
    indents = "   " * indent_level
    info_str = f"{indents}{': '.join(str(x) for x in data)}"
    _LOGGER.info(info_str)


def log_error(data: list, indent_level: int) -> None:
    indents = "   " * indent_level
    info_str = f"{indents}{': '.join(str(x) for x in data)}"
    _LOGGER.error(info_str)


def log_debug(data: list, indent_level: int) -> None:
    indents = "   " * indent_level
    info_str = f"{indents}{' '.join(str(x) for x in data)}"
    _LOGGER.debug(info_str)


def setup_platform(hass, config, add_devices, discovery_info=None):
    """Get the public transport sensor."""

    data = PublicTransportData(
        trip_update_url=config.get(CONF_TRIP_UPDATE_URL),
        vehicle_position_url=config.get(CONF_VEHICLE_POSITION_URL),
        route_delimiter=config.get(CONF_ROUTE_DELIMITER),
        api_key=config.get(CONF_API_KEY),
        x_api_key=config.get(CONF_X_API_KEY),
        static_gtfs_url=config.get(CONF_GTFS_URL),
    )

    sensors = []
    for departure in config.get(CONF_DEPARTURES):
        sensors.append(
            PublicTransportSensor(
                data=data,
                stop=departure.get(CONF_STOP_ID),
                route=departure.get(CONF_ROUTE),
                direction=departure.get(CONF_DIRECTION_ID),
                icon=departure.get(CONF_ICON),
                service_type=departure.get(CONF_SERVICE_TYPE),
                name=departure.get(CONF_NAME),
                # TODO add ROUTE_NAME and STOP_CODE
            )
        )

    add_devices(sensors)


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
            trip_update_df, timetable_df, how="outer", indicator="source"
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
            lambda row: row["live_arrival_time"]
            if row["live_arrival_time"] != 0
            else row["arrival_time"]
            + row["start_date"]
            + pd.to_timedelta(row["arrival_delay"], unit="s"),
            axis=1,
        )

        # return completed dataframe
        self.info_df = trip_update_df


class PublicTransportSensor(Entity):
    """Implementation of a public transport sensor."""

    def __init__(
        self,
        data: PublicTransportData,
        stop: str,
        route: str,
        direction: int,
        icon: str,
        service_type: str,
        name: str,
    ):
        """Initialize the sensor."""
        self.data = data
        self._name = name
        self._stop = stop
        self._route = route
        self._direction = direction
        self._icon = icon
        self._service_type = service_type
        self.next_services = pd.DataFrame()
        self.update()

    @property
    def name(self):
        return self._name

    def _get_next_services(
        self,
    ) -> pd.DataFrame:
        log_debug(["Filtering data..."], 0)
        # this filter is occasionally returning nothing for some reason
        # - maybe a datatype matching issue? Or agressive filtering?
        filtered_df = self.data.info_df[
            (self.data.info_df["stop_id"] == self._stop)
            & (self.data.info_df["direction_id"] == self._direction)
            & (self.data.info_df["route_id"] == self._route)
        ].sort_values(by=["arrival_time"], ascending=True)

        log_debug([debug_dataframe(filtered_df, "Filtered data")], 0)
        return filtered_df

    @property
    def state(self):
        """Return the state of the sensor."""
        return (
            due_in_mins(self.next_services.iloc[0]["stop_time"])
            if len(self.next_services) > 0
            else "-"
        )

    @property
    def extra_state_attributes(self):
        # """Return the state attributes."""
        ATTR_NEXT_UP = "Next " + self._service_type
        attrs = {
            ATTR_DUE_IN: self.state,
            ATTR_STOP_ID: self._stop,
            ATTR_ROUTE: self._route,
            ATTR_DIRECTION_ID: self._direction,
        }
        if len(self.next_services) > 1:
            second_service = self.next_services.iloc[1]
            attrs[ATTR_NEXT_UP] = second_service["stop_time"].strftime(
                TIME_STR_FORMAT
            )
        else:
            attrs[ATTR_NEXT_UP] = "-"

        if len(self.next_services) > 0:
            next_service = self.next_services.iloc[0]
            attrs[ATTR_DUE_AT] = next_service["stop_time"].strftime(
                TIME_STR_FORMAT
            )
            attrs[ATTR_LATITUDE] = next_service["vehicle_latitude"]
            attrs[ATTR_LONGITUDE] = next_service["vehicle_longitude"]
        else:
            attrs[ATTR_DUE_AT] = "-"
            attrs[ATTR_LATITUDE] = "-"
            attrs[ATTR_LONGITUDE] = "-"

        return attrs

    @property
    def unit_of_measurement(self):
        """Return the unit this state is expressed in."""
        return "min"

    @property
    def icon(self):
        return self._icon

    @property
    def service_type(self):
        return self._service_type

    def update(self):
        """Get the latest data from GTFS API and update the states."""
        self.data.update()
        self.next_services = self._get_next_services()

        # Logging Sensor Update Info
        log_info(["Sensor Update:"], 0)

        attributes = [
            ["Name", self._name],
            [ATTR_ROUTE, self._route],
            [ATTR_STOP_ID, self._stop],
            [ATTR_DIRECTION_ID, self._direction],
            [ATTR_ICON, self._icon],
            ["Service Type", self._service_type],
            ["unit_of_measurement", self.unit_of_measurement],
            [ATTR_DUE_IN, self.state],
        ]
        for attribute in attributes:
            log_info(attribute, 1)

        extra_attributes = [
            ATTR_DUE_AT,
            ATTR_LATITUDE,
            ATTR_LONGITUDE,
            f"Next {self._service_type}",
        ]
        for extra_att in extra_attributes:
            try:
                log_info(
                    [extra_att, self.extra_state_attributes[extra_att]], 1
                )
            except KeyError:
                log_info([extra_att, "not defined"], 1)


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


def debug_dataframe(df: pd.DataFrame, name: str = "") -> str:
    try:
        df_info_df = pd.concat(
            objs=[
                df.sample(n=min(4, df.shape[0])).transpose(),
                df.dtypes,
                df.memory_usage(deep=True),
            ],
            axis=1,
        )
    except ValueError:
        df_info_df = df.dtypes

    df_string = (
        f"\n\nTransposed Dataframe - {name} "
        f"{df_info_df.to_string(line_width=79, show_dimensions=True,)}\n"
        # f"{df.head(30).to_string()}"
    )
    return df_string
