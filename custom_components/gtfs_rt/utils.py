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
    max_rows = 8
    # sort columns alphabetically
    df = df[sorted(df.columns)]
    try:
        df_info_df = pd.concat(
            objs=[
                df.sample(n=min(max_rows, df.shape[0])).transpose(),
                df.dtypes,
                df.memory_usage(deep=True),
            ],
            axis=1,
        )
    except ValueError:
        df_info_df = df.dtypes

    df_string = df_info_df.to_string(
        # line_width=79,
        max_colwidth=20,
        show_dimensions=True,
    )

    debug_string = (
        f"\n\nTransposed Dataframe - {name} "
        f"(Rows: {df.shape[0]}, Columns: {df.shape[1]})\n"
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
        diff = int(time - now)
        return int(diff / 60)
    except ValueError as e:
        log_error(["Error calculating time difference", e, now, time], 1)
        return -1


def unix_to_str_timestamp(time, time_format: str) -> str:
    if isinstance(time, float):
        time = pd.to_datetime(time, unit="s", utc=True).tz_convert(
            tz=tz.tzlocal()
        )
        return time.strftime(time_format)
    else:
        return "-"


def remove_duplicated_columns(
    df: pd.DataFrame,
    column_names: list[str],
    x_suffix: str,
    y_suffix: str,
) -> pd.DataFrame:
    """
    Remove specified duplicate columns from a dataframe after merging.

    :param df: Dataframe to modify.
    :type df: pd.DataFrame
    :param column_names: List of column names that are duplicated.
    :type column_names: list[str]
    :param x_suffix: Suffix appended to data from "left" dataframe.
    :type x_suffix: str
    :param y_suffix: Suffix appended to data from "right" dataframe.
    :type y_suffix: str
    :return: Dataframe with duplicate columns removed, favouring the data from
    the "left" dataframe unless that is NA.
    :rtype: pd.DataFrame
    """

    # If column from X is NA, fill with value from Y
    for column in column_names:
        df[column] = (
            df[f"{column}_{x_suffix}"]
            .astype(str)
            .fillna(df[f"{column}_{y_suffix}"].astype(str))
        )
    # drop intermediate columns
    return df.drop(
        columns=[f"{c}_{x_suffix}" for c in column_names]
        + [f"{c}_{y_suffix}" for c in column_names]
    )
