var analyticsData;
var interval_id;

// function setParams() {
//     build_id = document.querySelector("[property~=build_id][content]").content;
//     testId = document.querySelector("[property~=test_id][content]").content;
//     lg_type = document.querySelector("[property~=lg_type][content]").content;
//     test_name = document.querySelector("[property~=test_name][content]").content;
//     environment = document.querySelector("[property~=environment][content]").content;
//     samplerType = $("#sampler").val().toUpperCase();
//     statusType = $("#status").val().toLowerCase();
//     aggregator = $("#aggregator").val().toLowerCase();
// }


function fillSummaryTable() {
    $.get(
        '/api/v1/backend_performance/charts/requests/table', {
            build_id: build_id,
            test_name: test_name,
            lg_type: lg_type,
            sampler: samplerType,
            status: statusType,
            start_time: $("#start_time").html(),
            end_time: $("#end_time").html(),
            low_value,
            high_value,
        },
        function(data) {
            data.forEach((item) => {
                $('#summary_table').bootstrapTable('append', item)
            })
        });
}

function loadRequestData(url, y_label) {
    if (!$("#preset").is(":visible")) {
        $("#preset").show();
        $("#analytics").hide();
        $("#chartjs-custom-legend-analytic").hide();
        if (analyticsLine != null) {
            analyticsLine.destroy();
        }
    }
    // if ($("#end_time").html() != "") {
    //     $("#PP").hide();
    // }
    $.get(
        url, {
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
        function(data) {
            const lineChartData = data;
            if (window.presetLine != null) {
                window.presetLine.destroy();
            }
            drawCanvas(y_label, lineChartData);
            document.getElementById('chartjs-custom-legend').innerHTML = window.presetLine.generateLegend();
        }
    );
}

function displayAnalytics() {
    console.log("displayAnalytics ***************")
    $("#preset").hide();
    analyticsCanvas();
    $("#analytics").show();
    $("#chartjs-custom-legend-analytic").show();
    if(window.presetLine!=null){
        window.presetLine.destroy();
    }
    if ( ! $("#analytics").is(":visible") ) {
        console.log("Here")
    }
}

function getData(scope, request_name) {
    if (! $(`#${request_name}_${scope}`).is(":checked")) {
        findAndRemoveDataSet(`${request_name}_${scope}`);
    } else {
        getDataForAnalysis(scope, request_name)
    }
}

function findAndRemoveDataSet(dataset_name){
    for (var i=0; i<analyticsLine.data.datasets.length; i++) {
        if (analyticsLine.data.datasets[i].label === dataset_name) {
            analyticsLine.data.datasets.splice(i, 1);
            analyticsLine.update();
            break;
        }
    }
}

function switchSampler() {
    samplerType = $("#sampler").val().toUpperCase();
    resizeChart();
}

function switchStatus() {
    statusType = $("#status").val().toLowerCase();
    resizeChart();
}

function switchAggregator() {
    aggregator = $("#aggregator").val();
    resizeChart();
}

function selectOrUnselectRequests() {
    if ($('#all_checkbox').is(":checked")) {
        $('.custom__checkbox').each(function(i, ch) {
            if (ch.id != "all_checkbox") {
                $('#' + ch.id).prop('checked', true);
            }
        });
        updateHiddenProperty(false);
    } else {
        $('.custom__checkbox').each(function(i, ch) {
            if (ch.id != "all_checkbox") {
                $('#' + ch.id).prop('checked', false);
            }
        });
        updateHiddenProperty(true);
    }
}

function updateHiddenProperty(hidden) {
    var ci = window.presetLine;
    for (let index = 1; index < ci.data.datasets.length; ++index) {
        var curr = ci.data.datasets[index]._meta;
        curr = Object.values(curr)[0]
        curr.hidden = hidden
    }
    ci.update();
}

updateChart = function(e, datasetIndex) {
    $('#all_checkbox').prop('checked', false);
    var index = datasetIndex;
    var ci = e.view.presetLine;
    var curr = ci.data.datasets[index]._meta;
    curr = Object.values(curr)[0]
    curr.hidden = !curr.hidden
    ci.update();
};

function updateChartAnalytic(e, datasetIndex) {
    var index = datasetIndex;
    var ci = e.view.analyticsLine;
    var curr = ci.data.datasets[index]._meta;
    curr = Object.values(curr)[0]
    curr.hidden = !curr.hidden
    ci.update();
};

function turnOnAllLine() {
    window.analyticsLine.data.datasets.forEach((item, index) => {
        let curr = item._meta;
        curr = Object.values(curr)[0];
        curr.hidden = false;
    })
    window.analyticsLine.update();
}
function analyticsCanvas() {
    console.log("analyticsCanvas ******************")
    var analyticsContext=document.getElementById("chart-analytics").getContext("2d");
    window.analyticsLine = Chart.Line(analyticsContext, {
        data: analyticsData,
        options: {
            responsive: true,
            hoverMode: 'index',
            stacked: false,
            legendCallback: (chart) => {
                return chart.data.datasets.map((item, index) => {
                    return `
                        <div class="d-flex mb-3 float-left mr-3">
                            <label class="mb-0 w-100 d-flex align-items-center custom-checkbox custom-checkbox__multicolor legend-item">
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
            title:{
                display: false,
            },
            scales: {
                yAxes: [{
                    type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
                    display: true,
                    position: "left",
                    scaleLabel: {
                        display: true,
                        labelString: "Response Time, ms"
                    },
                    id: "time",
                }, {
                    type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
                    display: true,
                    position: "right",
                    scaleLabel: {
                        display: true,
                        labelString: "Count"
                    },
                    id: "count",
                    gridLines: {
                        drawOnChartArea: false, // only want the grid lines for one axis to show up
                    },
                }],
            }
        }
    });
}

function drawCanvas(y_label, chartData) {
    const presetsContext = document.getElementById("chart-requests").getContext("2d");
    window.presetLine = Chart.Line(presetsContext, {
        data: chartData,
        options: {
            responsive: true,
            hoverMode: 'index',
            stacked: false,
             legendCallback: function (chart) {
                var legendHtml = [];
                for (var i=0; i<chart.data.datasets.length; i++) {
                    if (chart.data.datasets[i].label != "Active Users") {
                        var cb = '<div class="d-flex mb-3">';
                        cb += '<label class="mb-0 w-100 d-flex align-items-center custom-checkbox custom-checkbox__multicolor">'
                        cb += '<input class="mx-2 custom__checkbox" id="'+ chart.legend.legendItems[i].datasetIndex +'" type="checkbox" checked="true" style="--cbx-color: ' + chart.data.datasets[i].backgroundColor + ';" '
                        cb += 'onclick="updateChart(event, ' + '\'' + chart.legend.legendItems[i].datasetIndex + '\'' + ')"/>';
                        cb += '<span class="custom-chart-legend-span"></span>'
                        cb += chart.data.datasets[i].label;
                        cb += '</label></div>'
                        legendHtml.push(cb);
                    }
                }
                return legendHtml.join("");
            },
            legend: {
                display: false,
                position: 'right',
                labels: {
                    fontSize: 10,
                    usePointStyle: false
                }
            },
            title: {
                display: false,
            },
            scales: {
                xAxes: [{
                    gridLines: {
                        display: false
                    }
                }],
                yAxes: [{
                    type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
                    display: true,
                    position: "left",
                    scaleLabel: {
                        display: true,
                        labelString: y_label
                    },
                    id: "response_time",
                    gridLines: {
                        borderDash: [2, 1],
                        color: "#D3D3D3"
                    },
                    ticks: {
                        beginAtZero: true,
                        maxTicksLimit: 10
                    },
                }, {
                    type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
                    display: true,
                    position: "right",
                    gridLines: {
                        display: false
                    },
                    ticks: {
                        beginAtZero: true,
                        maxTicksLimit: 10
                    },
                    id: "active_users",
                }],
            }
        }
    });
}

// function fillErrorTable() {
//     var start_time = $("#start_time").html()
//     var end_time = $("#end_time").html()
//     test_name = document.querySelector("[property~=test_name][content]").content;
//     $('#errors').bootstrapTable('refreshOptions', {
//         url: `/api/v1/backend_performance/charts/errors/table?test_name=${test_name}&start_time=${start_time}&end_time=${end_time}&low_value=${low_value}&high_value=${high_value}`
//     })
// }

const filtersBlock = new Map();

function recalculateAnalytics() {
    const objRequest = new Map()
    filtersBlock.forEach(items => {
        items.forEach(requestName => {
            const metric = requestName.split('_').pop();
            const transaction = requestName.split('_').slice(0, -1).join('_');
            if(objRequest.get(metric)) {
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
$.get(
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
  function( data ) {
    if (analyticsLine.data.labels.length === 0 || analyticsLine.data.labels.length !== data.labels.length)
    {
        analyticsData = data;
        analyticsCanvas();
    } else {
        const uniqueDatasets = data.datasets.filter(item => {
            const isValueNotExist = analyticsLine.data.datasets.some(currentItem => currentItem.label === item.label)
            if(!isValueNotExist) return item
        })
        analyticsLine.data.datasets.push(...uniqueDatasets);
        analyticsLine.update();
    }
    turnOnAllLine();
    document.getElementById('chartjs-custom-legend-analytic').innerHTML = analyticsLine.generateLegend();
  }
 );
}

function resizeChart() {
    if ($("#analytics").is(":visible")) {
        analyticsData = null;
        analyticsLine.destroy();
        analyticsCanvas();
        recalculateAnalytics();
    }
    ["RT", "AR", "HT", "AN"].forEach(item => {
        if ($(`#${item}`).hasClass("active")) {
            $(`#${item}`).trigger("click");
        }
    });
    fillErrorTable();
}

function detailFormatter(index, row) {
    var html = []
    html.push('<p><b>Method:</b> ' + row['Method'] + '</p>')
    html.push('<p><b>Request Params:</b> ' + row['Request params'] + '</p>')
    html.push('<p><b>Headers:</b> ' + row['Headers'] + '</p>')
    html.push('<p><b>Response body:</b></p>')
    html.push('<textarea disabled style="width: 100%">' + row['Response body'] + '</textarea>')
    return html.join('')
}

// function showConfig() {
//     //TODO
//     console.log("show test config")
// }

function rerunTest() {
    //TODO
    console.log("rerun test with the same config")
}

function setBaseline() {
    var data = {
        test_name: test_name,
        env: environment,
        build_id: build_id
    };

    $.ajax({
        url: `/api/v1/backend_performance/baseline/${getSelectedProjectId()}`,
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(data)
    });
}

function compareWithBaseline() {
    console.log("here")
    $.get(
        `/api/v1/backend_performance/baseline/${getSelectedProjectId()}`, {
            test_name: test_name,
            env: environment
        },
        function(data) {
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

function stopTest() {
    data = {
        "test_status": {
            "status": "Canceled",
            "percentage": 100,
            "description": "Test was canceled"
        }
    }
    $.ajax({
        url: `/api/v1/backend_performance/report_status/${getSelectedProjectId()}/${testId}`,
        data: JSON.stringify(data),
        contentType: 'application/json',
        type: 'PUT',
        success: function(result) {
            document.location.reload();
        }
    });
}

function auto_update() {
    var interval = parseInt($('#auto_update').val())
    if (interval_id != null || interval == 0) {
        clearInterval(interval_id);
    }
    if (interval != 0) {
        interval_id = setInterval(function() {
            updateChartAndErrorsTable(interval_id);
        }, interval);
    }
}

function updateChartAndErrorsTable(interval_id) {
    if ($("#sampler").val() == null) {
        samplerType = "Request"
    } else {
        samplerType = $("#sampler").val().toUpperCase();
    }

    statusType = $("#status").val().toLowerCase();
    aggregator = $("#aggregator").val().toLowerCase();

    $.get(
    `/api/v1/backend_performance/report_status/${getSelectedProjectId()}/${testId}`,
    function(data) {
        var status = data["message"]
        if (!['finished', 'error', 'failed', 'success'].includes(status.toLowerCase())) {
            const sections = ['#RT', '#AR', '#HT', "#AN"];
            sections.forEach(element => {
                if ($(element).hasClass("active")) {
                    $(element).trigger( "click" )
                }
            });
            fillErrorTable();
        } else {
            clearInterval(interval_id);
        }
    });
}

function clearAnalyticChart() {
    analyticsLine.data.datasets = [];
    analyticsLine.update();
    document.getElementById('chartjs-custom-legend-analytic').innerHTML = '';
}

function handleAnalytic(e) {
    let isDisabled = false;
    e.target.classList.forEach(item => {
        if(item === 'disabled') {
            isDisabled = true
        }
    })
    if(!isDisabled) displayAnalytics()
}