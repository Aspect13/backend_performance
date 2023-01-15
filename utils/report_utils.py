from collections import defaultdict
from datetime import datetime
from functools import partial
from typing import Tuple, Union

from .utils import str_to_timestamp
from ..connectors.minio import calculate_auto_aggregation as calculate_auto_aggregation_minio
from ..connectors.influx import calculate_auto_aggregation as calculate_auto_aggregation_influx

from tools import data_tools, constants as c

from pylon.core.tools import log


def create_dataset(timeline, data, scope, metric, axe):
    labels = []
    for _ in timeline:
        labels.append(datetime.strptime(_, "%Y-%m-%dT%H:%M:%SZ").strftime("%m-%d %H:%M:%S"))
    datasets = []
    colors = data_tools.charts.color_gen(len(scope))
    for each, color in zip(scope, colors):
        datasets.append({
            "label": f"{each}_{metric}",
            "fill": False,
            "data": list(data.values()),
            "yAxisID": axe,
            "borderWidth": 2,
            "lineTension": 0,
            "spanGaps": True,
            "backgroundColor": "rgb({}, {}, {})".format(*color),
            "borderColor": "rgb({}, {}, {})".format(*color)
        })
    return {
        "labels": labels,
        "datasets": datasets
    }


def _create_dataset(timeline, data, scope, metric, axe):
    labels = []
    for _ in timeline:
        labels.append(datetime.strptime(_, "%Y-%m-%dT%H:%M:%SZ").strftime("%m-%d %H:%M:%S"))
    datasets = []
    colors = data_tools.charts.color_gen(len(data))
    for each, color in zip(data, colors):
        key = list(each.keys())[0]
        datasets.append({
            "label": f"{key}_{metric}",
            "fill": False,
            "data": list(each[key].values()),
            "yAxisID": axe,
            "borderWidth": 2,
            "lineTension": 0,
            "spanGaps": True,
            "backgroundColor": "rgb({}, {}, {})".format(*color),
            "borderColor": "rgb({}, {}, {})".format(*color)
        })
    return {
        "labels": labels,
        "datasets": datasets
    }


def comparison_data(timeline, data):
    labels = []
    for _ in timeline:
        labels.append(datetime.strptime(_, "%Y-%m-%dT%H:%M:%SZ").strftime("%m-%d %H:%M:%S"))
    chart_data = {
        "labels": labels,
        "datasets": []
    }
    colors = data_tools.charts.color_gen(len(data))
    for record, color in zip(data, colors):
        dataset = {
            "label": record,
            "fill": False,
            "data": list(data[record][0].values()),
            "yAxisID": data[record][1],
            "borderWidth": 2,
            "lineTension": 0,
            "spanGaps": True,
            "backgroundColor": "rgb({}, {}, {})".format(*color),
            "borderColor": "rgb({}, {}, {})".format(*color)
        }
        chart_data["datasets"].append(dataset)
    return chart_data


def chart_data(timeline, users, other, yAxis="response_time", convert_time: bool = True) -> dict:
    if convert_time:
        labels = []
        try:
            for t in timeline:
                labels.append(datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").strftime("%m-%d %H:%M:%S"))
                # labels.append(t)
        except ValueError:
            labels = timeline
    else:
        labels = timeline
    _data = {
        "labels": labels,
        "datasets": []
    }
    if users:
        _data['datasets'].append({"label": "Active Users", "fill": False,
                                  "data": list(users['users'].values()),
                                  "yAxisID": "active_users", "pointRadius": 0,
                                  "borderColor": "rgb(94,114,228)",
                                  "borderWidth": 2, "lineTension": 0.1, "spanGaps": True})
    colors = data_tools.charts.color_gen(len(other.keys()))
    for each, color in zip(other, colors):
        dataset = {
            "label": each,
            "fill": False,
            "yAxisID": yAxis,
            "borderWidth": 1,
            "lineTension": 0.2,
            "pointRadius": 1,
            "spanGaps": True,
            "backgroundColor": "rgb({}, {}, {})".format(*color),
            "borderColor": "rgb({}, {}, {})".format(*color),
            "data": []
        }
        for _ in timeline:
            dumb_fix = str(_) + 'Z'
            if dumb_fix in other[each]:
                dataset['data'].append(other[each][dumb_fix])
            else:
                dataset['data'].append(None)
        _data['datasets'].append(dataset)
    return _data


def render_analytics_control(requests: list) -> dict:
    item = {
        "Users": "getData('Users', '{}')",
        # "Hits": "getData('Hits', '{}')",
        "Throughput": "getData('Throughput', '{}')",
        "Errors": "getData('Errors', '{}')",
        "Min": "getData('Min', '{}')",
        "Median": "getData('Median', '{}')",
        "Max": "getData('Max', '{}')",
        "pct90": "getData('pct90', '{}')",
        "pct95": "getData('pct95', '{}')",
        "pct99": "getData('pct99', '{}')",
        "1xx": "getData('1xx', '{}')",
        "2xx": "getData('2xx', '{}')",
        "3xx": "getData('3xx', '{}')",
        "4xx": "getData('4xx', '{}')",
        "5xx": "getData('5xx', '{}')"
    }
    control = defaultdict(dict)
    for each in ["All", *requests]:
        for every in item:
            control[each][every] = item[every].format(each)
    return control


def calculate_proper_timeframe(build_id: str, test_name: str, lg_type: str, low_value: int, high_value: int,
                               start_time: datetime, end_time: datetime, aggregation: str,
                               time_as_ts: bool = False, source: str = None) -> Union[Tuple[str, str, str], Tuple[int, int, str]]:
    start_time_ts = start_time.timestamp()
    end_time_ts = end_time.timestamp()

    interval = end_time_ts - start_time_ts
    start_shift = interval * (float(low_value) / 100.0)
    end_shift = interval * (float(high_value) / 100.0)

    end_time_ts = start_time_ts + end_shift
    start_time_ts += start_shift
    if time_as_ts:
        return int(start_time_ts), int(end_time_ts), aggregation
    # t_format = "%Y-%m-%dT%H:%M:%S.000Z"
    # start_time = datetime.fromtimestamp(start_time_ts).strftime(t_format)
    # end_time = datetime.fromtimestamp(end_time).strftime(t_format)
    _start_time = datetime.utcfromtimestamp(start_time_ts).isoformat(sep=' ', timespec='seconds')
    _end_time = datetime.utcfromtimestamp(end_time_ts).isoformat(sep=' ', timespec='seconds')
    if aggregation == 'auto' and build_id:
        if source == 'minio':
            aggregation = calculate_auto_aggregation_minio(build_id, test_name, lg_type,
                                                           start_time=_start_time,
                                                           end_time=_end_time)
        else:
            aggregation = calculate_auto_aggregation_influx(build_id, test_name, lg_type,
                                                            start_time=_start_time,
                                                            end_time=_end_time)
    return _start_time, _end_time, aggregation
