import homeassistant.helpers.config_validation as cv

# import homeassistant.util.dt as dt_util
import pandas as pd
import voluptuous as vol
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import ATTR_LATITUDE, ATTR_LONGITUDE, CONF_NAME
from homeassistant.helpers.entity import Entity
from PublicTransportData import PublicTransportData
from utils import get_time_delta, log_debug, log_info, unix_to_str_timestamp

ATTR_STOP_ID = "Stop ID"
ATTR_ROUTE_ID = "Route ID"
ATTR_DIRECTION_ID = "Direction ID"
ATTR_DUE_IN = "Due in"
ATTR_DUE_AT = "Due at"
ATTR_NEXT_UP = "Next Service"
ATTR_ICON = "Icon"
ATTR_SENSOR_NAME = "Sensor Name"
ATTR_STOP_CODE = "Stop Code"
ATTR_STOP_NAME = "Stop Name"
ATTR_ROUTE_NO = "Route Number"
ATTR_NEXT_ARRIVAL_TIME = "Next Arrival"
ATTR_SERVICE_COUNT = "Upcoming Services"
ATTR_DEP_TIME = "Departure Time"
ATTR_RT_FLAG = "Live Update"
ATTR_DELAY = "Delay"
ATTR_VEHICLE_ID = "Vehicle ID"
ATTR_SERVICE_TYPE = "Service Type"
ATTR_UNITS = "units"

CONF_API_KEY = "api_key"
CONF_X_API_KEY = "x_api_key"
CONF_STOP_ID = "stopid"
CONF_ROUTE = "route"
CONF_DIRECTION_ID = "directionid"
CONF_DEPARTURES = "departures"
CONF_OPERATOR = "operator"
CONF_TRIP_UPDATE_URL = "trip_update_url"
CONF_VEHICLE_POSITION_URL = "vehicle_position_url"
CONF_ROUTE_DELIMITER = "route_delimiter"
CONF_ICON = "icon"
CONF_SERVICE_TYPE = "service_type"
CONF_LIMIT = "arrivals_limit"

# these parameters are new for static GTFS integration
CONF_GTFS_URL = "gtfs_url"
CONF_ROUTE_NO = "route_name"
CONF_STOP_CODE = "stop_code"
# end of new parameters

DEFAULT_SERVICE = "Service"
DEFAULT_ICON = "mdi:bus"
DEFAULT_DIRECTION = 1

TIME_STR_FORMAT = "%H:%M"

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_TRIP_UPDATE_URL): cv.string,
        vol.Optional(CONF_API_KEY): cv.string,
        vol.Optional(CONF_X_API_KEY): cv.string,
        vol.Optional(CONF_VEHICLE_POSITION_URL): cv.string,
        vol.Optional(CONF_ROUTE_DELIMITER): cv.string,
        vol.Required(CONF_GTFS_URL): cv.string,
        vol.Optional(CONF_LIMIT, default=30): vol.Coerce(int),  # type: ignore
        vol.Optional(CONF_DEPARTURES): [
            {
                vol.Required(CONF_NAME): cv.string,
                vol.Optional(
                    CONF_STOP_ID, default=""  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_ROUTE, default=""  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_DIRECTION_ID,
                    default=DEFAULT_DIRECTION,  # type: ignore
                ): int,
                vol.Optional(
                    CONF_ICON, default=DEFAULT_ICON  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_SERVICE_TYPE, default=DEFAULT_SERVICE  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_ROUTE_NO, default=""  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_STOP_CODE, default=""  # type: ignore
                ): cv.string,
                vol.Optional(
                    CONF_OPERATOR, default=""  # type: ignore
                ): cv.string,
            }
        ],
    }
)


class PublicTransportSensor(Entity):
    """Implementation of a public transport sensor."""

    def __init__(
        self,
        data: PublicTransportData,
        stop_id: str,
        route_id: str,
        direction: int,
        icon: str,
        service_type: str,
        sensor_name: str,
        route_no: str,
        stop_code: str,
        operator: str,
        arrivals_limit: int,
    ):
        """Initialize the sensor."""
        self.data = data
        self._sensor_name = sensor_name
        self._stop_id = stop_id
        self._route_id = route_id
        self._direction = direction
        self._icon = icon
        self._service_type = service_type
        self._route_no = route_no
        self._stop_code = stop_code
        self._operator = operator
        self._arrivals_limit = arrivals_limit
        self.next_services = pd.DataFrame()
        self.update()

    @property
    def name(self):
        return self._sensor_name

    def _get_next_services(
        self,
    ) -> dict:
        filters = {
            "stop_id": self._stop_id,
            "direction_id": self._direction,
            "route_id": self._route_id,
            "route_short_name": self._route_no,
            "stop_code": self._stop_code,
            "agent_id": self._operator,
        }

        return self.data.filter_df(
            filters,
            order_by="arrival_time",
            order_ascending=True,
            limit=self._arrivals_limit,
        )

    @property
    def state(self):
        """Return the state of the sensor."""
        return (
            get_time_delta(self.next_services[0]["stop_time"])
            if len(self.next_services) > 0
            else "-"
        )

    @property
    def extra_state_attributes(self):
        """Return the state attributes."""
        # TODO - add more attributes
        # TODO - assign route/stop attributes based on services dict if not
        #  defined

        # Get details for this service
        service_count = len(self.next_services)
        arrival_time = "-"
        departure_time = "-"
        arrival_delay = "-"
        latitude = "-"
        longitude = "-"
        rt_flag = "-"
        vehicle_id = "-"
        route_id = "-"
        route_no = "-"
        stop_id = "-"
        stop_code = "-"
        stop_name = "-"
        direction_id = "-"
        # get details for next service
        next_arrival_time = "-"

        # TODO - cache vehicle position data to allow for no position update?
        if service_count > 0:
            next_service = self.next_services[0]

            route_id = next_service["route_id"]
            route_no = next_service["route_short_name"]
            stop_id = next_service["stop_id"]
            stop_code = next_service["stop_code"]
            stop_name = next_service["stop_name"]
            direction_id = next_service["direction_id"]

            arrival_time = next_service["stop_time_dt"].strftime(
                TIME_STR_FORMAT
            )
            latitude = next_service["vehicle_latitude"]
            longitude = next_service["vehicle_longitude"]
            # departure_time = ?
            # rt_flag = ?
            arrival_delay = int(next_service["arrival_delay"] / 60)
            vehicle_id = next_service["vehicle_id"]

        # Get details for the service after next
        if service_count > 1:
            second_service = self.next_services[1]
            next_arrival_time = second_service["stop_time_dt"].strftime(
                TIME_STR_FORMAT
            )
        ATTR_NEXT_ARRIVAL_TIME = f"Next {self._service_type}"
        attrs = {
            ATTR_SENSOR_NAME: self.name,
            ATTR_DUE_AT: arrival_time,
            ATTR_DEP_TIME: departure_time,
            ATTR_DUE_IN: self.state,
            ATTR_DELAY: arrival_delay,
            ATTR_UNITS: self.unit_of_measurement,
            ATTR_ROUTE_ID: route_id,
            ATTR_ROUTE_NO: route_no,
            ATTR_STOP_ID: stop_id,
            ATTR_STOP_CODE: stop_code,
            ATTR_STOP_NAME: stop_name,
            ATTR_DIRECTION_ID: direction_id,
            ATTR_VEHICLE_ID: vehicle_id,
            ATTR_LATITUDE: latitude,
            ATTR_LONGITUDE: longitude,
            ATTR_ICON: self.icon,
            ATTR_NEXT_ARRIVAL_TIME: next_arrival_time,
            ATTR_RT_FLAG: rt_flag,
            ATTR_SERVICE_TYPE: self._service_type,
            ATTR_SERVICE_COUNT: service_count,
        }

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
        log_debug(["Updating sensor..."], 0)
        self.data.update()
        self.next_services = self._get_next_services()
        # Logging Sensor Update Info
        log_info(["\nSensor Update:"], 0)

        attributes = self.extra_state_attributes
        for att in attributes:
            try:
                log_info([att, attributes[att]], 1)
            except KeyError:
                log_info([att, "not defined"], 1)


def setup_platform(
    hass, config, add_devices, discovery_info=None
) -> list[PublicTransportSensor]:
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
                stop_id=departure.get(CONF_STOP_ID),
                route_id=departure.get(CONF_ROUTE),
                direction=departure.get(CONF_DIRECTION_ID),
                icon=departure.get(CONF_ICON),
                service_type=departure.get(CONF_SERVICE_TYPE),
                sensor_name=departure.get(CONF_NAME),
                route_no=departure.get(CONF_ROUTE_NO),
                stop_code=departure.get(CONF_STOP_CODE),
                operator=departure.get(CONF_OPERATOR),
                arrivals_limit=config.get(CONF_LIMIT),
            )
        )

    add_devices(sensors)
    return sensors
