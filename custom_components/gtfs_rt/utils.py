import logging
from datetime import datetime

import pandas as pd
from dateutil import tz

_LOGGER = logging.getLogger(__name__)


def log_info(data: list, indent_level: int) -> None:
    indents = "   " * indent_level
    info_str = f"\t{indents}{': '.join(str(x) for x in data)}"
    _LOGGER.info(info_str)


def log_error(data: list, indent_level: int) -> None:
    indents = "   " * indent_level
    info_str = f"{indents}{': '.join(str(x) for x in data)}"
    _LOGGER.error(info_str)


def log_debug(data: list, indent_level: int) -> None:
    indents = "   " * indent_level
    info_str = f"{indents}{' '.join(str(x) for x in data)}"
    _LOGGER.debug(info_str)


def debug_dataframe(df: pd.DataFrame, name: str = "") -> None:
    """
    Logs information about the specified dataframe and displays some
    sample entries.

    :param df: Dataframe to provide information on.
    :type df: pd.DataFrame
    :param name: Label to show in log, defaults to "".
    :type name: str, optional
    """
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

    df_string = df_info_df.to_string(
        line_width=79,
        max_colwidth=20,
        show_dimensions=True,
    )

    debug_string = (
        f"\n\nTransposed Dataframe - {name} "
        f"(Rows: {df.shape[0]}, Columns: {df.shape[1]})"
        f"{df_string}\n"
    )
    log_debug([debug_string], 0)


def get_time_delta(time: float) -> int:
    """
    Get the remaining minutes from now until a given datetime object.

    :param time: Time to compare to now.
    :type time: datetime
    :return: Minutes between now and "time".
    :rtype: int
    """
    local_timezone = tz.tzlocal()
    now = datetime.now(tz=local_timezone).timestamp()
    try:
        diff = int(time - now)  # dt_util.now().replace(tzinfo=None)
        log_debug(
            [
                "Calculated time difference",
                int(diff / 60),
                now,
                time,
            ],
            1,
        )
        return int(diff / 60)
    except ValueError as e:
        log_error(["Error calculating time difference", e, now, time], 1)
        return -1
