const get_responses_chart = (mount_id, y_label, chartData) => {
    const chart_options = {
        type: 'line',
        data: chartData,
        options: {
            animation: false,
            responsive: true,
            // hoverMode: 'index',
            // interaction: {
            //     mode: 'point'
            // },
            plugins: {
                legend: {
                    display: false
                },
                title: {
                    display: false
                }
            },
            // title: {
            //     display: false,
            // },
            // scales: {
            //     xAxes: [
            //         {
            //             gridLines: {
            //                 display: false
            //             }
            //         }
            //     ],
            //     yAxes: [
            //         {
            //             type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
            //             display: true,
            //             position: "left",
            //             scaleLabel: {
            //                 display: true,
            //                 labelString: y_label
            //             },
            //             id: "response_time",
            //             gridLines: {
            //                 borderDash: [2, 1],
            //                 color: "#D3D3D3"
            //             },
            //             ticks: {
            //                 beginAtZero: true,
            //                 maxTicksLimit: 10
            //             },
            //         },
            //         {
            //             type: "linear", // only linear but allow scale type registration. This allows extensions to exist solely for log scale for instance
            //             display: true,
            //             position: "right",
            //             gridLines: {
            //                 display: false
            //             },
            //             ticks: {
            //                 beginAtZero: true,
            //                 maxTicksLimit: 10
            //             },
            //             // id: "active_users",
            //         }
            //     ],
            // }
            scales: {
                x: {
                    type: 'time',
                    grid: {
                        display: false
                    }
                },
                response_time: {
                    type: 'linear',
                    position: 'left',
                    text: y_label,
                    display: true,
                    grid: {
                        display: true,
                        borderDash: [2, 1],
                        color: "#D3D3D3"
                    },
                    ticks: {
                        count: 10
                    }
                },
                active_users: {
                    type: 'linear',
                    position: 'right',
                    min: 0,
                    grid: {
                        display: false,
                        drawOnChartArea: false,
                    },
                    // ticks: {
                    //     count: 10
                    // }
                }
            }
        },
        plugins: []
    }
    // const presetsContext = document.getElementById("chart-requests").getContext("2d");
    return new Chart(mount_id, chart_options);
}