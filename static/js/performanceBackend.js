window.analyticsLine = undefined

function displayAnalytics() {
    $("#preset").hide();
    analyticsCanvas();
    $("#analytics").show();
    $("#chartjs-custom-legend-analytic").show();
    if (!$("#analytics").is(":visible")) {
        console.log("Here")
    }

}

function getData(scope, request_name) {
    if (!$(`#${request_name}_${scope}`).is(":checked")) {
        findAndRemoveDataSet(`${request_name}_${scope}`);
    } else {
        getDataForAnalysis(scope, request_name)
    }
}

function findAndRemoveDataSet(dataset_name) {
    for (let i = 0; i < analyticsLine.data.datasets.length; i++) {
        if (analyticsLine.data.datasets[i].label === dataset_name) {
            analyticsLine.data.datasets.splice(i, 1);
            analyticsLine.update();
            break;
        }
    }
}

function updateChartAnalytic(e, datasetIndex) {
    const curr = Object.values(e.view.analyticsLine.data.datasets[datasetIndex]._meta)[0]
    curr.hidden = !curr.hidden
    e.view.analyticsLine.update();
}

function highlight(e, datasetIndex, chartType) {
    var ci = chartType === 'analytic' ? e.view.analyticsLine : e.view.presetLine;
    ci.data.datasets[datasetIndex].borderWidth = 6;
    ci.update();
}

function lowlight(e, datasetIndex, chartType) {
    var ci = chartType === 'analytic' ? e.view.analyticsLine : e.view.presetLine;
    ci.data.datasets[datasetIndex].borderWidth = 2;
    ci.update();
}

function turnOnAllLine() {
    window.analyticsLine.data.datasets.forEach((item, index) => {
        // let curr = item._meta;
        // curr = Object.values(curr)[0];
        // curr.hidden = false;
        window.analyticsLine.setDatasetVisibility(index, true)
    })
    window.analyticsLine.update();
}

function analyticsCanvas(data) {
    console.log("analyticsCanvas ******************")
    // var analyticsContext=document.getElementById("").getContext("2d");
    window.analyticsLine !== undefined && window.analyticsLine.destroy()
    window.analyticsLine = new Chart('chart-analytics', {
        type: 'line',
        // window.analyticsLine = Chart.Line('chart-analytics', {
        data: data,
        options: {
            animation: false,
            responsive: true,
            hoverMode: 'index',
            stacked: false,
            legendCallback: (chart) => {
                return chart.data.datasets.map((item, index) => {
                    return `
                        <div class="d-flex mb-3 float-left mr-3">
                            <label 
                                onmouseover="highlight(event, ${chart.legend.legendItems[index].datasetIndex}, 'analytic')"
                                onmouseout="lowlight(event, ${chart.legend.legendItems[index].datasetIndex}, 'analytic')"
                                class="mb-0 w-100 d-flex align-items-center custom-checkbox custom-checkbox__multicolor legend-item">
                                <input class="mx-2 custom__checkbox"
                                    onclick="updateChartAnalytic(event, ${chart.legend.legendItems[index].datasetIndex})"
                                    id="${chart.legend.legendItems[index].datasetIndex}"
                                    type="checkbox"
                                    checked="true"
                                    style="--cbx-color: ${item.backgroundColor}"/>
                                <span class="legend-span">${item.label}</span>
                            </label>
                        </div>
                            `
                }).join("")
            },
            legend: {
                display: false,
                position: 'bottom',
                labels: {
                    fontSize: 10,
                    usePointStyle: false
                }
            },
            title: {
                display: false,
            },
            // scales: {
            //     yAxes: [{
            //         type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
            //         display: true,
            //         position: "left",
            //         scaleLabel: {
            //             display: true,
            //             labelString: "Response Time, ms"
            //         },
            //         id: "time",
            //     }, {
            //         type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
            //         display: true,
            //         position: "right",
            //         scaleLabel: {
            //             display: true,
            //             labelString: "Count"
            //         },
            //         id: "count",
            //         gridLines: {
            //             drawOnChartArea: false, // only want the grid lines for one axis to show up
            //         },
            //     }],
            // }
        }
    });
}


const filtersBlock = new Map();

function recalculateAnalytics() {
    const objRequest = new Map()
    filtersBlock.forEach(items => {
        items.forEach(requestName => {
            const metric = requestName.split('_').pop();
            const transaction = requestName.split('_').slice(0, -1).join('_');
            if (objRequest.get(metric)) {
                objRequest.set(metric, [...objRequest.get(metric), transaction])
            } else {
                objRequest.set(metric, [transaction])
            }
        })
    })
    objRequest.forEach((requestArray, metric) => {
        getDataForAnalysis(metric, requestArray)
    })
}

function getDataForAnalysis(metric, request_name) {
    const controller_proxy = vueVm.registered_components.summary
    $.get(
        '/api/v1/backend_performance/charts/requests/data',
        {
            scope: request_name,
            metric: metric,
            build_id: controller_proxy.build_id,
            test_name: controller_proxy.test_name,
            lg_type: controller_proxy.lg_type,
            sampler: controller_proxy.sampler_type,
            aggregator: controller_proxy.aggregator,
            status: controller_proxy.status_type,
            start_time: controller_proxy.start_time,
            end_time: controller_proxy.end_time,
            low_value: controller_proxy.slider.low,
            high_value: controller_proxy.slider.high,
        },
        function (data) {
            if (analyticsLine.data.labels.length === 0 || analyticsLine.data.labels.length !== data.labels.length) {
                // analyticsData = data;
                analyticsCanvas(data);
            } else {
                const uniqueDatasets = data.datasets.filter(item => {
                    const isValueNotExist = analyticsLine.data.datasets.some(currentItem => currentItem.label === item.label)
                    if (!isValueNotExist) return item
                })
                analyticsLine.data.datasets.push(...uniqueDatasets);
                analyticsLine.update();
            }
            turnOnAllLine();
            // document.getElementById('chartjs-custom-legend-analytic').innerHTML = analyticsLine.generateLegend();
            document.getElementById('chartjs-custom-legend-analytic').innerHTML = Chart.defaults.plugins.legend.labels.generateLabels(analyticsLine)
        }
    );

}
async function getDataForAnalysis2 (metric, request_name) {
    return await $.get(
        '/api/v1/backend_performance/charts/requests/data',
        {
            scope: request_name,
            metric: metric,
            build_id: build_id,
            test_name: test_name,
            lg_type: lg_type,
            sampler: samplerType,
            aggregator: aggregator,
            status: statusType,
            start_time: $("#start_time").html(),
            end_time: $("#end_time").html(),
            low_value,
            high_value,
        },
        function (data) {
            if (data.datasets && data.datasets.length > 0) {
                return data.datasets.filter(item => {
                    const isValueNotExist = analyticsLine.data.datasets.some(currentItem => currentItem.label === item.label)
                    if (!isValueNotExist) return item
                });
            }
        }
    );
}
function compareWithBaseline() {
    console.log("here")
    $.get(
        `/api/v1/backend_performance/baseline/${getSelectedProjectId()}`, {
            test_name: test_name,
            env: environment
        },
        function (data) {
            if (data['baseline'].length != 0) {
                var baseline_id = data['report_id']
                const queryString = window.location.search;
                const urlParams = new URLSearchParams(queryString);
                var report_id = urlParams.get('result_test_id');
                if (report_id == baseline_id) {
                    console.log("Current test is Baseline")
                    $("#compare_with_baseline").html('Current test is Baseline');
                } else {
                    // TODO add comparison page
                    //var url = window.location.origin + "/report/compare?id[]=" + baseline_id + "&id[]=" + report_id;
                    //window.location.href = url;
                    console.log("Compare two reports")
                    $("#compare_with_baseline").html('Compare two reports');
                }

            } else {
                console.log("Baseline is not set yet")
                $("#compare_with_baseline").html('Baseline is not set yet');
            }
        }
    );
}

function setThresholds() {
    //TODO
    console.log("set current report results as threshold")
}

function downloadReport() {
    //TODO
    console.log("download test report")
}

function shareTestReport() {
    //TODO
    console.log("share test report")
}


function clearAnalyticChart() {
    analyticsLine.data.datasets = [];
    analyticsLine.update();
    document.getElementById('chartjs-custom-legend-analytic').innerHTML = '';
}