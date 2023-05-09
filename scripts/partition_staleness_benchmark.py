# ruff: noqa: T201
#!/usr/bin/env python

import operator
from datetime import datetime, timedelta
from functools import reduce
from itertools import groupby
from random import randint
from typing import Dict, Mapping

from dagster import HourlyPartitionsDefinition
from dagster._core.definitions.asset_graph import AssetGraph
from dagster._core.definitions.data_version import CachingStaleStatusResolver
from dagster._core.definitions.decorators.asset_decorator import asset
from dagster._core.definitions.materialize import materialize
from dagster._core.definitions.partition_key_range import PartitionKeyRange
from dagster._core.definitions.time_window_partitions import DailyPartitionsDefinition
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.instance_for_test import instance_for_test
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from typing_extensions import Literal

START_DATE = datetime(2020, 1, 1)
NUMBER_DAYS = 5

hourly_partitions = HourlyPartitionsDefinition(start_date=START_DATE)
daily_partitions = DailyPartitionsDefinition(start_date=START_DATE)


def get_range_tags(num_days: int, scheme: Literal["hourly", "daily"]) -> Mapping[str, str]:
    key_range = get_range(num_days, scheme)
    return {
        ASSET_PARTITION_RANGE_START_TAG: key_range.start,
        ASSET_PARTITION_RANGE_END_TAG: key_range.end,
    }


def get_range(num_days: int, scheme: Literal["hourly", "daily"]) -> PartitionKeyRange:
    suffix = "-00:00" if scheme == "hourly" else ""
    day_delta = timedelta(days=num_days) if scheme == "hourly" else timedelta(days=num_days - 1)
    start = START_DATE.strftime(f"%Y-%m-%d{suffix}")
    end = (START_DATE + day_delta).strftime(f"%Y-%m-%d{suffix}")
    return PartitionKeyRange(start, end)


def partition_key_for_day(day: int) -> str:
    str_day = str(day).zfill(2)
    return f"2020-01-{str_day}"


def partition_key_for_day_and_hour(day: int, hour: int) -> str:
    str_hour = str(hour).zfill(2)
    return f"{partition_key_for_day(day)}-{str_hour}:00"


@asset(partitions_def=hourly_partitions)
def hourly_asset(context: OpExecutionContext):
    keys = hourly_partitions.get_partition_keys_in_range(context.asset_partition_key_range)
    return {key: randint(0, 100) for key in keys}


@asset(partitions_def=daily_partitions)
def daily_asset(context, hourly_asset):
    groups = groupby(hourly_asset.keys(), lambda key: key.rsplit("-", 1)[0])
    value: Dict[str, int] = {}
    for day, group in groups:
        hour_vals = [hourly_asset[k] for k in group]
        value[day] = reduce(operator.add, hour_vals)
    return value


asset_graph = AssetGraph.from_assets([daily_asset, hourly_asset])
hourly_keys = hourly_partitions.get_partition_keys_in_range(get_range(NUMBER_DAYS, "hourly"))
daily_keys = hourly_partitions.get_partition_keys_in_range(get_range(NUMBER_DAYS, "hourly"))

with instance_for_test() as instance:

    def time_resolution() -> timedelta:
        resolver = CachingStaleStatusResolver(
            instance=instance, asset_graph=asset_graph, resolve_partitions=True
        )
        start_time = datetime.now()
        for key in daily_keys:
            resolver.get_status(daily_asset.key, key)
        end_time = datetime.now()
        return end_time - start_time

    materialize([hourly_asset], tags=get_range_tags(NUMBER_DAYS, "hourly"), instance=instance)
    hourly_source = hourly_asset.to_source_asset()
    materialize(
        [daily_asset, hourly_source], tags=get_range_tags(NUMBER_DAYS, "daily"), instance=instance
    )

    before_time = time_resolution()
    materialize(
        [daily_asset, hourly_source], tags=get_range_tags(NUMBER_DAYS, "daily"), instance=instance
    )
    after_time = time_resolution()
    print(f"Time to resolve stale status for {len(daily_keys)} daily partitions:")
    print(f"  Before: {before_time}")
    print(f"  After: {after_time}")
