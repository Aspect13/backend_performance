// let low_value = 0
// let high_value = 100
const result_test_id = new URLSearchParams(location.search).get('result_id')

const reRunTest = () => {
    fetch(`/api/v1/backend_performance/rerun/${result_test_id}`, {
        method: 'POST'
    }).then(response => {
        if (response.ok) {
            response.json().then(({result_id}) => {
                // search.set('result_test_id', result_id)
                alertMain.add(
                    `Test rerun successful! Check out the <a href="?result_id=${result_id}">result page</a>`,
                    'success-overlay',
                    true
                )
            })
        } else {
            response.text().then(data => {
                alertMain.add(data, 'danger')
            })
        }
    })
}


const setBaseline = async () => {
    await fetch(`/api/v1/backend_performance/baseline/${getSelectedProjectId()}`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            test_name: test_name,
            env: environment,
            build_id: build_id
        })

    })
}


const SummaryController = {
    props: ['samplers', 'start_time', 'end_time', 'test_name', 'initial_status_percentage', 'lg_type', 'build_id'],
    delimiters: ['[[', ']]'],
    data() {
        return {
            slider: {
                low: 0,
                high: 100
            },
            sampler_type: '',
            status_type: 'all',
            // todo: change to 'auto'
            aggregator: '30s',
            // aggregator: 'auto',
            update_interval: 0,
            auto_update_id: null,
            status_percentage: 0,
            active_tab_id: undefined
        }
    },
    async mounted() {
        this.status_percentage = this.initial_status_percentage
        this.sampler_type = this.samplers.length > 0 ? this.samplers[0] : ''
        $(() => {
            noUiSlider.create($("#vuh-perfomance-time-picker")[0], {
                range: {
                    'min': 0,
                    'max': 100
                },
                start: [0, 100],
                connect: true,
                format: wNumb({
                    decimals: 0
                }),
            }).on('set', this.handle_slider_change)
            const observer = new IntersectionObserver((entries, observer) => {
                entries.forEach((entry) => {
                    if (entry.intersectionRatio === 0) {
                        $(entry.target).css("height", $(entry.target).height())
                        $(entry.target).children().css("display", "none")
                    } else {
                        $(entry.target).css("height", "")
                        $(entry.target).children().css("display", "block")
                    }
                })
            }, {
                root: null,
                rootMargin: '0px',
                threshold: 0
            })
            observer.observe(document.getElementById('under-summary-controller'))
        })
        this.fill_error_table()
        this.poll_test_status()

        $('#summary_table').bootstrapTable('refresh', {
            url: '/api/v1/backend_performance/charts/requests/table?' + new URLSearchParams({
                build_id: this.build_id,
                test_name: this.test_name,
                lg_type: this.lg_type,
                sampler: this.sampler_type,
                start_time: this.start_time,
                end_time: this.end_time,
                low_value: this.slider.low,
                high_value: this.slider.high
            })
        })
        // await this.load_request_data('/api/v1/backend_performance/charts/requests/summary', "Response time, ms")
        this.active_tab_id = $('#pills-tab a.active').attr('id')

    },
    watch: {
        async active_tab_id(new_value) {
            console.log('watch active tab', new_value)
            await this.handle_tab_load(new_value)
        }
    },
    methods: {
        async handle_tab_load(tab_id) {
            switch (tab_id) {
                case 'AR':
                    await this.load_request_data(
                        '/api/v1/backend_performance/charts/requests/average',
                        'Response time, ms'
                    )
                    break
                case 'HT':
                    await this.load_request_data(
                        '/api/v1/backend_performance/charts/requests/hits',
                        'Hits/Requests per second'
                    )
                    break
                case 'AN':
                    // todo: check if disabled??? or wtf was that function
                    displayAnalytics()
                    break
                case 'RT':
                default:
                    await this.load_request_data(
                        '/api/v1/backend_performance/charts/requests/summary',
                        'Response time, ms'
                    )
            }
        },
        async handle_slider_change(values) {
            [this.slider.low, this.slider.high] = values
            // this.resizeChart()
            await this.handle_tab_load(this.active_tab_id)
            this.fill_error_table()
        },
        handle_tab_change(event) {
            this.active_tab_id = event.target.id
        },
        async handle_status_change(event) {
            this.status_type = event.target.value
            await this.handle_tab_load(this.active_tab_id)
        },
        async handle_aggregator_change(event) {
            this.aggregator = event.target.value
            await this.handle_tab_load(this.active_tab_id)
        },
        async handle_sampler_change(event) {
            this.sampler_type = event.target.value
            await this.handle_tab_load(this.active_tab_id)
        },
        handle_change_update_interval(event) {
            this.update_interval = parseInt(event.target.value)
            if (this.auto_update_id != null) {
                clearInterval(this.auto_update_id)
                this.auto_update_id = null
            }
            if (this.update_interval > 0) {
                this.auto_update_id = setInterval(async () => {
                        // if ($("#sampler").val() == null) {
                        //     samplerType = "Request"
                        // } else {
                        //     samplerType = $("#sampler").val().toUpperCase();
                        // }

                        // statusType = $("#status").val().toLowerCase();
                        // aggregator = $("#aggregator").val().toLowerCase();
                        const resp = await fetch(`/api/v1/backend_performance/report_status/${getSelectedProjectId()}/${testId}`)
                        if (resp.ok) {
                            const {message} = await resp.json()
                            // var status = data["message"]
                            if (!['finished', 'error', 'failed', 'success'].includes(message.toLowerCase())) {
                                const sections = ['#RT', '#AR', '#HT', "#AN"]
                                sections.forEach(element => {
                                    const $element = $(element)
                                    $element.hasClass("active") && $element.trigger("click")
                                });
                                this.fillErrorTable()
                            } else {
                                clearInterval(this.auto_update_id)
                                this.auto_update_id = null
                            }
                        }
                    },
                    this.update_interval
                )
            }
        },
        fill_error_table() {
            $('#errors').bootstrapTable('refresh', {
                url: '/api/v1/backend_performance/charts/errors/table?' + new URLSearchParams({
                    test_name: this.test_name,
                    start_time: this.start_time,
                    end_time: this.end_time,
                    low_value: this.slider.low,
                    high_value: this.slider.high,
                })
            })
        },
        async poll_test_status() {
            if (this.status_percentage < 100) {
                $('#AN').addClass('disabled')
                $('#analytic-loader').show()
                const resp = await fetch(`/api/v1/backend_performance/reports/${getSelectedProjectId()}/?report_id=${result_test_id}`)
                if (resp.ok) {
                    const {test_status: {percentage}} = await resp.json()
                    this.status_percentage = percentage
                    setTimeout(this.poll_test_status, 5000)
                } else {
                    // todo: handle fetch error
                }

            } else {
                $('#AN').removeClass('disabled');
                $('#analytic-loader').hide()
            }
        },
        resizeChart() {
            if ($("#analytics").is(":visible")) {
                // analyticsData = null;
                analyticsLine.destroy();
                analyticsCanvas(null);
                recalculateAnalytics();
            }
            // ["RT", "AR", "HT", "AN"].forEach(item => {
            //     const $item = $(`#${item}`)
            //     $item.hasClass("active") && $item.trigger("click")
            // });
            // fillErrorTable();
            // this.fill_error_table()
        },
        async load_request_data(url, y_label) {
            $('#chart-loader').show();
            const $preset = $("#preset")
            if (!$preset.is(":visible")) {
                $preset.show();
                $("#analytics").hide();
                $("#chartjs-custom-legend-analytic").hide();
                if (analyticsLine != null) {
                    analyticsLine.destroy();
                }
            }
            // if ($("#end_time").html() != "") {
            //     $("#PP").hide();
            // }
            const resp = await fetch(url + '?' + new URLSearchParams({
                build_id: this.build_id,
                test_name: this.test_name,
                lg_type: this.lg_type,
                sampler: this.sampler_type,
                aggregator: this.aggregator,
                status: this.status_type,
                start_time: this.start_time,
                end_time: this.end_time,
                low_value: this.slider.low,
                high_value: this.slider.high,
            }))
            if (resp.ok) {
                const data = await resp.json()
                if (window.presetLine === undefined) {
                    window.presetLine = get_responses_chart('chart-requests', y_label, data)
                } else {
                    window.presetLine.data = data
                    window.presetLine.update()
                }
                // if (window.presetLine != null) {
                //     // window.presetLine.destroy();
                // } else {
                //     //
                // }
                // drawCanvas(y_label, data);

                $('#chart-loader').hide();
                // document.getElementById('chartjs-custom-legend').innerHTML = window.presetLine.generateLegend();
                // document.getElementById('chartjs-custom-legend').innerHTML = Chart.defaults.plugins.legend.labels.generateLabels(window.presetLine)
            } else {
                // todo: handle fetch error
            }
        }
    },
    template: `<slot :master="this"></slot>`
}

register_component('SummaryController', SummaryController)

function errors_detail_formatter(index, row) {
    return `
        <p><b>Method:</b>${row['Method']}</p>
        <p><b>Request Params:</b>${row['Request params']}</p>
        <p><b>Headers:</b>${row['Headers']}</p>
        <p><b>Response body:</b></p>
        <textarea disabled style="width: 100%">${row['Response body']}</textarea>
    `
}

// show config modal
$(document).on('vue_init', () => {
    const disable_inputs = () => {
        $('#config_modal span[contenteditable]').attr('contenteditable', false)
        $('#config_modal input').attr('disabled', true)
        $('#config_modal input[type=text]').attr('readonly', true)
        $('#config_modal button').attr('disabled', true)
        $('#config_modal button[data-toggle=collapse]').attr('disabled', false)
        $('#config_modal button[data-dismiss=modal]').attr('disabled', false)
    }
    disable_inputs()
    $('#show_config_btn').on('click', disable_inputs)
})

// init sequence
$(document).on('vue_init', () => {
    $(() => {

        // getTestStatus()
        // setParams();
        // loadRequestData('/api/v1/backend_performance/charts/requests/summary', "Response time, ms");
        // fillErrorTable();

    })
})

