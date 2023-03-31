import logging

import pandas as pd

_LOGGER = logging.getLogger(__name__)


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
    )
    return df_string
