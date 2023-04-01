import homeassistant.helpers.config_validation as cv

# import homeassistant.util.dt as dt_util
import pandas as pd
import voluptuous as vol
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import ATTR_LATITUDE, ATTR_LONGITUDE, CONF_NAME
from homeassistant.helpers.entity import Entity
from PublicTransportData import PublicTransportData
from utils import get_time_delta, log_debug, log_info

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
    ) -> dict:
        filters = {
            "stop_id": self._stop,
            # "direction_id": self._direction,
            "route_id": self._route,
        }

        return self.data.filter_df(
            filters, order_by="arrival_time", order_ascending=True
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
        # """Return the state attributes."""
        ATTR_NEXT_UP = "Next " + self._service_type
        attrs = {
            ATTR_DUE_IN: self.state,
            ATTR_STOP_ID: self._stop,
            ATTR_ROUTE: self._route,
            ATTR_DIRECTION_ID: self._direction,
        }
        if len(self.next_services) > 1:
            second_service = self.next_services[1]
            attrs[ATTR_NEXT_UP] = second_service["stop_time"].strftime(
                TIME_STR_FORMAT
            )
        else:
            attrs[ATTR_NEXT_UP] = "-"

        if len(self.next_services) > 0:
            next_service = self.next_services[0]
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
        log_debug(["Updating sensor..."], 0)
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
