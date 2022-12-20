from datetime import datetime, timezone
from typing import Callable, Optional, Generator

from ..models.api_reports import APIReport
from ..connectors.influx import (
    get_backend_requests, get_hits_tps, average_responses, get_build_data,
    get_tps_for_analytics, get_response_codes_for_analytics, get_backend_users,
    get_throughput_per_test, get_response_time_per_test,
    get_errors_for_analytics, get_backend_requests_for_analytics,
    get_engine_health_cpu, get_project_id, get_engine_health_memory, get_engine_health_load
)

from ..connectors.minio import get_requests_summary_data
from ..connectors.loki import get_results
from .report_utils import calculate_proper_timeframe, chart_data, create_dataset, comparison_data, _create_dataset
from pylon.core.tools import log

from tools import constants as c, influx_tools, data_tools


def _timeframe(args: dict, time_as_ts: bool = False) -> tuple:
    log.info(f"args {args}")
    end_time = args.get('end_time')
    high_value = args.get('high_value', 100)
    if not end_time:
        end_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        high_value = 100
    return calculate_proper_timeframe(args.get('build_id', None), args['test_name'], args.get('lg_type', None),
                                      args.get('low_value', 0),
                                      high_value, args['start_time'], end_time, args.get('aggregator', 'auto'),
                                      time_as_ts=time_as_ts)


def _query_only(args: dict, query_func: Callable) -> dict:
    start_time, end_time, aggregation = _timeframe(args)
    timeline, results, users = query_func(
        args['build_id'], args['test_name'], args['lg_type'],
        start_time, end_time, aggregation,
        sampler=args['sampler'], status=args["status"]
    )
    return chart_data(timeline, users, results, convert_time=args.get('convert_time', True))


def get_tests_metadata(tests):
    tests_meta = APIReport.query.filter(APIReport.id.in_(tests)).order_by(APIReport.id.asc()).all()
    users_data = {}
    responses_data = {}
    errors_data = {}
    rps_data = {}
    labels = []

    for each in tests_meta:
        ts = datetime.fromtimestamp(c.str_to_timestamp(each.start_time),
                                    tz=timezone.utc).strftime("%m-%d %H:%M:%S")
        labels.append(ts)
        users_data[ts] = each.vusers
        responses_data[ts] = each.pct95
        errors_data[ts] = each.failures
        rps_data[ts] = each.throughput
    return labels, rps_data, errors_data, users_data, responses_data


def requests_summary(args: dict):
    args['convert_time'] = False
    # log.info("args***********")
    # log.info(args)
    # log.info("********************")
    # log.info("influx response")
    # _res = _query_only(args, get_backend_requests)
    # log.info(_res)
    # is_finished = True
    # if is_finished:
    #     _res = get_requests_summary_data(args)
    #     log.info("minio response")
    #     log.info(_res)
    # return _res
    return _query_only(args, get_backend_requests)


def requests_hits(args: dict):
    args['convert_time'] = False
    return _query_only(args, get_hits_tps)


def avg_responses(args: dict):
    args['convert_time'] = False
    return _query_only(args, average_responses)


def summary_table(args: dict):
    start_time, end_time, aggregation = _timeframe(args)
    return get_build_data(args['build_id'], args['test_name'], args['lg_type'], start_time, end_time, args['sampler'])


def get_issues(args: dict):
    start_time, end_time, aggregation = _timeframe(args, time_as_ts=True)
    return list(get_results(args['test_name'], start_time, end_time).values())


def calculate_analytics_dataset(build_id, test_name, lg_type, start_time, end_time, aggregation, sampler,
                                scope, metric, status):
    data = None
    axe = 'count'
    if metric == "Throughput":
        timestamps, data, _ = get_tps_for_analytics(build_id, test_name, lg_type, start_time, end_time, aggregation,
                                                    sampler, scope=scope, status=status)

    elif metric == "Errors":
        timestamps, data, _ = get_errors_for_analytics(build_id, test_name, lg_type, start_time, end_time, aggregation,
                                                       sampler, scope=scope)
    elif metric in ["Min", "Median", "Max", "pct90", "pct95", "pct99"]:
        timestamps, data, _ = get_backend_requests_for_analytics(build_id, test_name, lg_type, start_time, end_time,
                                                                 aggregation, sampler, scope=scope, aggr=metric,
                                                                 status=status)
        axe = 'time'

    elif "xx" in metric:
        timestamps, data, _ = get_response_codes_for_analytics(build_id, test_name, lg_type, start_time, end_time,
                                                               aggregation, sampler, scope=scope, aggr=metric,
                                                               status=status)
    return data, axe


def get_data_from_influx(args):
    start_time, end_time, aggregation = _timeframe(args)
    metric = args.get('metric', '')
    scope = args.get("scope[]", [])
    timestamps, users = get_backend_users(args['build_id'], args['lg_type'],
                                          start_time, end_time, aggregation)
    axe = 'count'
    if metric == "Users":
        return create_dataset(timestamps, users['users'], scope, metric, axe)
    data, axe = calculate_analytics_dataset(args['build_id'], args['test_name'], args['lg_type'],
                                            start_time, end_time, aggregation, args['sampler'],
                                            scope, metric, args["status"])
    if data:
        return _create_dataset(timestamps, data, scope, metric, axe)
    else:
        return {}


def prepare_comparison_responses(args):
    tests = args['id[]']
    tests_meta = []
    longest_test = 0
    longest_time = 0
    sampler = args.get('sampler', "REQUEST")
    for i in range(len(tests)):
        data = APIReport.query.filter_by(id=tests[i]).first().to_json()
        if data['duration'] > longest_time:
            longest_time = data['duration']
            longest_test = i
        tests_meta.append(data)
    start_time, end_time, aggregation = calculate_proper_timeframe(tests_meta[longest_test]['build_id'],
                                                                   tests_meta[longest_test]['name'],
                                                                   tests_meta[longest_test]['lg_type'],
                                                                   args.get('low_value', 0),
                                                                   args.get('high_value', 100),
                                                                   tests_meta[longest_test]['start_time'],
                                                                   tests_meta[longest_test]['end_time'],
                                                                   args.get('aggregator', 'auto'))
    # if args.get('aggregator', 'auto') != "auto":
    #     aggregation = args.get('aggregator')
    metric = args.get('metric', '')
    scope = args.get('scope', '')
    status = args.get("status", 'all')
    timestamps, users = get_backend_users(tests_meta[longest_test]['build_id'],
                                          tests_meta[longest_test]['lg_type'], start_time, end_time, aggregation)
    test_start_time = "{}_{}".format(tests_meta[longest_test]['start_time'].replace("T", " ").split(".")[0], metric)
    data = {test_start_time: calculate_analytics_dataset(tests_meta[longest_test]['build_id'],
                                                         tests_meta[longest_test]['name'],
                                                         tests_meta[longest_test]['lg_type'],
                                                         start_time, end_time, aggregation,
                                                         sampler, scope, metric, status)}
    for i in range(len(tests_meta)):
        if i != longest_test:
            test_start_time = "{}_{}".format(tests_meta[i]['start_time'].replace("T", " ").split(".")[0], metric)
            data[test_start_time] = calculate_analytics_dataset(tests_meta[i]['build_id'], tests_meta[i]['name'],
                                                                tests_meta[i]['lg_type'],
                                                                tests_meta[i]['start_time'],
                                                                tests_meta[i]['end_time'],
                                                                aggregation, sampler, scope, metric, status)
    return comparison_data(timeline=timestamps, data=data)


def compare_tests(args):
    labels, rps_data, errors_data, users_data, responses_data = get_tests_metadata(args['id[]'])
    return {
        "response": chart_data(labels, {"users": users_data}, {"pct95": responses_data}, "time"),
        "errors": chart_data(labels, {"users": users_data}, {"errors": errors_data}, "count"),
        "rps": chart_data(labels, {"users": users_data}, {"RPS": rps_data}, "count")
    }


def create_benchmark_dataset(args):
    build_ids = args['id[]']
    req = args.get('request')
    calculation = args.get('calculation')
    aggregator = args.get('aggregator')
    status = args.get("status", 'all')
    if not aggregator or aggregator == 'auto':
        aggregator = '1s'
    tests_meta = APIReport.query.filter(APIReport.id.in_(build_ids)).order_by(APIReport.vusers.asc()).all()
    labels = set()
    data = {}
    y_axis = ''
    for _ in tests_meta:
        try:
            labels.add(_.vusers)
            if _.environment not in data:
                data[_.environment] = {}
            if calculation == 'throughput':
                y_axis = 'Requests per second'
                data[_.environment][str(_.vusers)] = get_throughput_per_test(
                    _.build_id, _.name, _.lg_type, "", req, aggregator, status)
            elif calculation != ['throughput']:
                y_axis = 'Response time, ms'
                if calculation == 'errors':
                    y_axis = 'Errors'
                data[_.environment][str(_.vusers)] = get_response_time_per_test(
                    _.build_id, _.name, _.lg_type, "", req, calculation, status, aggregator)
            else:
                data[_.environment][str(_.vusers)] = None
        except IndexError:
            pass

    labels = [""] + sorted(list(labels)) + [""]
    return {"data": chart_data(labels, [], data, "data"), "label": y_axis}


def engine_health(args: dict, part: str = 'all') -> dict:
    part_mapping = {
        'cpu': engine_health_cpu,
        'memory': engine_health_memory,
        'load': engine_health_load,
    }
    d = dict(args)
    start_time, end_time, aggregation = _timeframe(d)
    d.update({
        'start_time': start_time,
        'end_time': end_time,
        'aggregation': aggregation,
    })
    func = part_mapping.get(part)
    if not func:  # e.g. part == 'all'
        project_id = get_project_id(args['build_id'])
        d['influx_client'] = influx_tools.get_client(project_id, f'telegraf_{project_id}')
        return {k: v(**d) for k, v in part_mapping.items()}
    return func(**d)


def generate_engine_health_dataset(host_name: str, series_data: list, data_structure: dict,
                                   palette: Optional[Generator] = None):
    for section, options in data_structure.items():
        dataset = {
            'data': list(map(lambda x: x[section], series_data)),
            # 'label': f'{section}:{host_name}',
            'label': section,
            'borderWidth': 2,
            'fill': False,
            'lineTension': 0.1,
            'pointRadius': 1,
            'spanGaps': True,
            'tag_host': host_name,
        }
        if palette:
            try:
                dataset['borderColor'] = 'rgb({}, {}, {})'.format(*next(palette))
            except StopIteration:
                dataset['borderColor'] = '#ffffff'
        dataset.update(options)
        yield dataset


def format_engine_health_data(health_data: dict, data_structure: dict) -> dict:
    datasets = []
    labels = []
    palette = data_tools.charts.color_gen(len(health_data.keys()) * len(data_structure.keys()))
    for host, series in health_data.items():
        if not labels:
            labels = list(map(lambda x: x['time'], series))

        datasets.extend(generate_engine_health_dataset(host, series, data_structure, palette))

    return {
        'datasets': datasets,
        'labels': labels,
        'hosts': list(health_data.keys()),
    }


def engine_health_cpu(**kwargs) -> dict:
    health_data = get_engine_health_cpu(**kwargs)
    structure = {
        'system': {
            'hidden': True
        },
        'user': {},
        'softirq': {
            'hidden': True
        },
        'iowait': {
            'hidden': True
        },
    }
    return format_engine_health_data(health_data, structure)


def engine_health_memory(**kwargs) -> dict:
    health_data = get_engine_health_memory(**kwargs)
    structure = {
        'heap memory': {},
        'non-heap memory': {},
    }
    return format_engine_health_data(health_data, structure)


def engine_health_load(**kwargs) -> dict:
    health_data = get_engine_health_load(**kwargs)
    structure = {
        'load1': {},
        'load5': {},
        'load15': {},
    }
    return format_engine_health_data(health_data, structure)
