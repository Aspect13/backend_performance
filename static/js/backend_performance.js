var test_formatters = {
    job_type(value, row, index) {
        if (row.job_type === "perfmeter") {
            return '<img src="/design-system/static/assets/ico/jmeter.png" width="20">'
        } else if (row.job_type === "perfgun") {
            return '<img src="/design-system/static/assets/ico/gatling.png" width="20">'
        } else {
            return value
        }
    },

    actions(value, row, index) {
        return `
            <div class="d-flex justify-content-end">
                <button type="button" class="btn btn-default btn-xs btn-table btn-icon__xs test_run mr-2" 
                        data-toggle="tooltip" data-placement="top" title="Run Test">
                    <i class="icon__18x18 icon-run"></i>
                </button>
                <div class="dropdown_multilevel">
                    <button class="btn btn-default btn-xs btn-table btn-icon__xs" type="button"
                            data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                        <i class="icon__18x18 icon-menu-dots"></i>
                    </button>
                    <ul class="dropdown-menu">
                        <li class="dropdown-menu_item dropdown-item d-flex align-items-center">
                            <span class="w-100 font-h5 d-flex align-items-center"><i class="icon__18x18 icon-integrate mr-1"></i>Integrate with</span>
                            <i class="icon__16x16 icon-sort"></i>
                            <ul class="submenu dropdown-menu">
                                <li class="dropdown-menu_item dropdown-item d-flex align-items-center int_docker">
                                    <span class="w-100 font-h5">Docker command</span>
                                </li>
                            </ul>
                        </li>
                        <li class="dropdown-menu_item dropdown-item d-flex align-items-center test_edit">
                            <i class="icon__18x18 icon-settings mr-2"></i><span class="w-100 font-h5">Settings</span>
                        </li>
                        <li class="dropdown-menu_item dropdown-item d-flex align-items-center test_delete">
                            <i class="icon__18x18 icon-delete mr-2"></i><span class="w-100 font-h5">Delete</span>
                        </li>
                    </ul>
                </div>
                
            </div>
        `
    },
    name_style(value, row, index) {
        return {
            css: {
                "max-width": "140px",
                "overflow": "hidden",
                "text-overflow": "ellipsis",
                "white-space": "nowrap"
            }
        }
    },
    cell_style(value, row, index) {
        return {
            css: {
                "min-width": "165px"
            }
        }
    },
    action_events: {
        "click .test_run": function (e, value, row, index) {
            // apiActions.run(row.id, row.name)
            console.log('test_run', row)
            const component_proxy = vueVm.registered_components.run_modal
            component_proxy.set({...row, test_parameters: [...JSON.parse(JSON.stringify(row.test_parameters))]})
        },

        "click .test_edit": function (e, value, row, index) {
            console.log('test_edit', row)
            const component_proxy = vueVm.registered_components.create_modal
            component_proxy.mode = 'update'
            component_proxy.set(row)
        },

        "click .test_delete": function (e, value, row, index) {
            console.log('test_delete', row)
            test_delete(row.id)

        },

        "click .int_docker": async function (e, value, row, index) {
            const resp = await fetch(`/api/v1/backend_performance/test/${row.project_id}/${row.id}/?output=docker`)
            if (resp.ok) {
                const {cmd} = await resp.json()
                vueVm.docker_command.cmd = cmd
                vueVm.docker_command.is_open = true
            } else {
                showNotify('ERROR', 'Error getting docker command')
            }
        }

    }
}

var report_formatters = {
    reportsStatusFormatter(value, row, index) {
        switch (value.status.toLowerCase()) {
            case 'error':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--red)"><i class="fas fa-exclamation-circle error"></i> ${value.status}</div>`
            case 'failed':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--red)"><i class="fas fa-exclamation-circle error"></i> ${value.status}</div>`
            case 'success':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--green)"><i class="fas fa-exclamation-circle error"></i> ${value.status}</div>`
            case 'canceled':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--gray)"><i class="fas fa-times-circle"></i> ${value.status}</div>`
            case 'finished':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--info)"><i class="fas fa-check-circle"></i> ${value.status}</div>`
            case 'in progress':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--basic)"><i class="fas fa-spinner fa-spin fa-secondary"></i> ${value.status}</div>`
            case 'post processing':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--basic)"><i class="fas fa-spinner fa-spin fa-secondary"></i> ${value.status}</div>`
            case 'pending...':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--basic)"><i class="fas fa-spinner fa-spin fa-secondary"></i> ${value.status}</div>`
            case 'preparing...':
                return `<div data-toggle="tooltip" data-placement="top" title="${value.description}" style="color: var(--basic)"><i class="fas fa-spinner fa-spin fa-secondary"></i> ${value.status}</div>`
            default:
                return value.status.toLowerCase()
        }
    },
    createLinkToTest(value, row, index) {
        return `<a class="test form-control-label font-h5" href="./results?result_id=${row.id}" role="button">${row.name}</a>`
    }
}

var custom_params_table_formatters = {
    input(value, row, index, field) {
        if (['test_name', 'test_type', 'env_type'].includes(row.name)) {
            return `
                <input type="text" class="form-control form-control-alternative" disabled
                    onchange="ParamsTable.updateCell(this, ${index}, '${field}')" value="${value}">
                <div class="invalid-tooltip invalid-tooltip-custom"></div>
            `
        }
        return ParamsTable.inputFormatter(value, row, index, field)

    },
    action(value, row, index, field) {
        if (['test_name', 'test_type', 'env_type'].includes(row.name)) {
            return ''
        }
        return ParamsTable.parametersDeleteFormatter(value, row, index)
    }
}

const CustomizationItem = {
    delimiters: ['[[', ']]'],
    props: ['file', 'path', 'i_index'],
    emits: ['update:file', 'update:path', 'delete'],
    template: `
    <div class="d-flex flex-row">
        <div class="flex-fill">
            <input type="text" class="form-control form-control-alternative" placeholder="bucket/file"
                @change="$emit('update:file', $event.target.value)"
                :value="file"
            >
        </div>
        <div class="flex-fill pl-3">
            <input type="text" class="form-control form-control-alternative" placeholder="path/to/file"
                @change="$emit('update:path', $event.target.value)"
                :value="path"
            >
        </div>
        <div class="m-auto pl-3">
            <button type="button" class="btn btn-32 btn-action" 
                @click="$emit('delete', i_index)"
            >
                <i class="fas fa-minus"></i>
            </button>
        </div>
    </div>
    `,
    methods: {
        initial_state() {
            return {
                file: '',
                path: '',
            }
        }
    }
}
const Customization = {
    delimiters: ['[[', ']]'],
    props: ['customization', 'errors'],
    emits: ['update:modelValue'],
    components: {
        CustomizationItem: CustomizationItem
    },
    data() {
        return {
            cc_items: []
        }
    },
    template: `
    <div class="card card-x card-row-1">
        <div class="card-header">
            <div class="d-flex flex-row">
                <div class="flex-fill">
                    <h9 class="flex-grow-1">Custom plugins and extensions</h9>
                    <p>
                        <h13>Bucket and file for your customizations</h13>
                    </p>
                </div>
                <div>
                    <button type="button" class="btn btn-32 btn-action mt-1"
                        @click="handle_add_item"
                    >
                        <i class="fas fa-plus"></i>
                    </button>
                </div>
            </div>
            <div class="d-flex flex-row invalid-feedback">[[ errors?.length > 0 ? errors[0]?.msg : '']]</div>
            <CustomizationItem
                v-for="(item, index) in cc_items"
                :key="index"
                :i_index="index"
                v-model:file="item.file"
                v-model:path="item.path"
                @delete="handle_delete_item"
            ></CustomizationItem>
        </div>
    </div>
    `,
    computed: {
        converted() {
            return this.cc_items.reduce((acc, i) => {
                acc[i.file] = i.path
                return acc
            }, {})
        }
    },
    methods: {
        handle_add_item() {
            this.cc_items.push(CustomizationItem.methods.initial_state())
            // this.$emit('update:modelValue', this.converted)
        },
        handle_delete_item(item_id) {
            this.cc_items.splice(item_id, 1)
            // this.$emit('update:modelValue', this.converted)
        },
        clear() {
            this.cc_items = []
        }
    },
    watch: {
        cc_items: {
            handler(newValue, oldValue) {
                this.$emit('update:modelValue', this.converted)
            },
            deep: true
        }
    },

}

const TestCreateModal = {
    delimiters: ['[[', ']]'],
    components: {
        Customization: Customization
    },
    props: ['modal_id', 'runners', 'test_params_id', 'source_card_id', 'locations'],
    template: `
<div class="modal modal-base fixed-left fade shadow-sm" tabindex="-1" role="dialog" :id="modal_id">
    <div class="modal-dialog modal-dialog-aside" role="document">
        <div class="modal-content">
            <div class="modal-header">
                <div class="row w-100">
                    <div class="col">
                        <h2>[[ mode === 'create' ? 'Create Backend Tests' : 'Update Backend Test' ]]</h2>
                    </div>
                    <div class="col-xs">
                        <button type="button" class="btn btn-secondary mr-2" data-dismiss="modal" aria-label="Close">
                            Cancel
                        </button>
                        <button type="button" class="btn btn-secondary mr-2" 
                            @click="() => handleCreate(false)"
                            v-if="mode === 'create'"
                        >
                            Save
                        </button>
                        <button type="button" class="btn btn-basic" 
                            @click="() => handleCreate(true)"
                            v-if="mode === 'create'"
                        >
                            Save and start
                        </button>
                        <button type="button" class="btn btn-secondary mr-2" 
                            @click="() => handleUpdate(false)"
                            v-if="mode === 'update'"
                        >
                            Update
                        </button>
                        <button type="button" class="btn btn-basic" 
                            @click="() => handleUpdate(true)"
                            v-if="mode === 'update'"
                        >
                            Update and start
                        </button>
                    </div>
                </div>
            </div>
            
            <slot name='alert_bar'></slot>


            <div class="modal-body">
                <div class="section">
                    <div class="row">
                        <div class="col">
                            <div class="form-group">
                                <p class="font-h5 font-semibold">Test Name</p>
                                <p>
                                    <h13>Enter a name that describes the purpose of your test.</h13>
                                </p>
                                <div class="custom-input mb-3 mt-1" 
                                    :class="{'invalid-input': errors?.name}">
                                    <input type="text"
                                        placeholder="Test Name"
                                        :disabled="mode !== 'create'"
                                        v-model='name'
                                        :class="{ 'disabled': mode !== 'create'}"
                                    >
                                    <span class="input_error-msg">[[ get_error_msg('name') ]]</span>
                                </div>
                            </div>
                            <div class="d-flex">
                                <div class="flex-fill">
                                    <div class="form-group">
                                        <p class="font-h5 font-semibold">Test Type</p>
                                        <p>
                                            <h13>Tag to group tests by type</h13>
                                        </p>
                                        <div class="custom-input mb-3 mt-1"
                                            :class="{ 'invalid-input': errors?.test_type }">
                                            <input type="text"
                                                placeholder="Test Type"
                                                v-model='test_type'
                                            >
                                            <span class="input_error-msg">[[ get_error_msg('test_type') ]]</span>
                                        </div>
                                    </div>
                                </div>
                                <div class="flex-fill">
                                    <div class="form-group">
                                        <p class="font-h5 font-semibold">Environment</p>
                                        <p>
                                            <h13>Tag to group tests by env</h13>
                                        </p>
                                        <div class="custom-input mb-3 mt-1"
                                            :class="{ 'invalid-input': errors?.env_type }">
                                            <input type="text"
                                                placeholder="Test Environment"
                                                v-model='env_type'
                                                >
                                            <span class="input_error-msg">[[ get_error_msg('env_type') ]]</span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="form-group" >
                                <p class="font-h5 font-semibold">Test runner</p>
                                <p>
                                    <h13>Choose the runner for the test.</h13>
                                </p>
                                <div class="custom-input w-100-imp">
                                    <select class="selectpicker bootstrap-select__b mt-1" data-style="btn" 
                                    v-model="runner"
                                    :class="{ 'is-invalid': errors?.runner }"
                                >
                                    
                                    <optgroup v-for='runner_group in Object.keys(runners).reverse()' :label="runner_group">
                                        <option v-for='runner in runners[runner_group]' :value="runner.version">
                                            [[ runner.name || runner_group + " " + runner.version ]]
                                        </option>
                                    </optgroup>
                                </select>
                                </div>
        
                                
                                <div class="invalid-feedback">[[ get_error_msg('runner') ]]</div>
                                <label class="mb-0 mt-1 w-100 d-flex align-items-center custom-checkbox"
                                    v-if="is_gatling_selected && active_source_tab === 'artifact'"
                                    >
                                        <input type="checkbox" class="mr-2"
                                            v-model='compile_tests'
                                            :class="{ 'is-invalid': errors?.compile_tests }"
                                            >
                                        <div class="invalid-feedback">[[ get_error_msg('compile_tests') ]]</div>
                                        <h9> Compile tests for Gatling </h9>
                                    </label>
                            </div>
                        </div>
                        <div class="col">
                            <slot name='sources'></slot>
                            
                            <div class="form-group mt-3">
                                <div class="form-group">
                                    <p class="font-h5 font-semibold">Entrypoint</p>
                                    <p>
                                        <h13>File for jMeter and class for gatling</h13>
                                    </p>
                                    <input type="text" class="form-control form-control-alternative"
                                           placeholder="Entrypoint (e.g. some.jmx or some.Test)"
                                           v-model='entrypoint'
                                           :class="{ 'is-invalid': errors?.entrypoint }"
                                           >
                                           <div class="invalid-feedback">[[ get_error_msg('entrypoint') ]]</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                
                <Locations 
                    v-model:location="location"
                    v-model:parallel_runners="parallel_runners"
                    v-model:cpu="cpu_quota"
                    v-model:memory="memory_quota"
                    v-model:cloud_settings="cloud_settings"
                    
                    modal_id="backend"
                    
                    v-bind="locations"
                    ref="locations"
                ></Locations>
                
                <slot name='params_table'></slot>
                <slot name='integrations'></slot>
                <slot name='scheduling'></slot>
                

                <div class="section mt-3" @click="handle_advanced_params_icon">
                    <div class="row" data-toggle="collapse" data-target="#advancedBackend" role="button" aria-expanded="false" aria-controls="advancedBackend">
                        <div class="col">
                            <h7>ADVANCED PARAMETERS</h7>
                            <p>
                                <h13>Configure parameters for test runner, test data and network setting</h13>
                            </p>
                        </div>
                        <div class="col">
                            <div class="col-xs text-right">
                                <button type="button" class="btn btn-nooutline-secondary mr-2"
                                        data-toggle="collapse" data-target="#advancedBackend">
                                        <i :class="advanced_params_icon"></i>
                                </button>
                            </div>
                        </div>
                    </div>
                    <div class="collapse row pt-4" id="advancedBackend"
                        ref="advanced_params"
                    >
                        <div class="col">
                            <Customization
                                v-model="customization"
                                ref="customization_component"
                                :errors="errors.customization"
                            ></Customization>
                            <div class="card card-x card-row-1" id="splitCSV">
                                <div class="card-header">
                                    <div class="d-flex flex-row">
                                        <div class="flex-fill">
                                            <h9 class="flex-grow-1">Split CSV</h9>
                                            <p>
                                                <h13>Distribute CSV data across load generators</h13>
                                            </p>
                                        </div>
                                        <div>
                                            <button type="button" class="btn btn-32 btn-action mt-1"
                                                    onclick="addCSVSplit('splitCSV')"><i class="fas fa-plus"></i>
                                            </button>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
    `,
    data() {
        return this.initial_state()
    },
    mounted() {
        $(this.$el).on('hide.bs.modal', this.clear)
        this.runner = this.default_runner
        $(this.source.el).find('a.nav-item').on('click', e => {
                this.active_source_tab = this.source.get_active_tab(e.target.id)
            }
        )
    },
    computed: {
        default_runner() {
            return this.$props.runners &&
                this.$props.runners[Object.keys(this.$props.runners).reverse()[0]][0].version
                || null
        },
        test_parameters() {
            return ParamsTable.Manager(this.$props.test_params_id)
        },
        source() {
            return SourceCard.Manager(this.$props.source_card_id)
        },
        integrations() {
            try {
                return IntegrationSection.Manager()
            } catch (e) {
                console.warn('No integration section')
                return undefined
            }
        },
        scheduling() {
            try {
                return SchedulingSection.Manager()
            } catch (e) {
                console.warn('No scheduling section')
                return undefined
            }
        },
        is_gatling_selected() {
            return Boolean(
                this.$props.runners.Gatling?.find(i => i.version === this.runner) !== undefined
            )
        }
    },
    watch: {
        errors(newValue,) {
            if (Object.keys(newValue).length > 0) {
                newValue.test_parameters ?
                    this.test_parameters.setError(newValue.test_parameters) :
                    this.test_parameters.clearErrors()
                newValue.source ?
                    this.source.setError(newValue.source) :
                    this.source.clearErrors()

                newValue.integrations ?
                    this.integrations?.setError(newValue.integrations) :
                    this.integrations?.clearErrors()

                newValue.scheduling ?
                    this.scheduling?.setError(newValue.scheduling) :
                    this.scheduling?.clearErrors()

                newValue.customization && $(this.$refs.advanced_params).collapse('show')
            } else {
                this.test_parameters.clearErrors()
                this.source.clearErrors()
                this.integrations?.clearErrors()
                this.scheduling?.clearErrors()
            }
        },
        is_gatling_selected(newValue) {
            if (!newValue) {
                this.compile_tests = false
            }
        }
    },
    methods: {
        get_error_msg(field_name) {
            return this.errors[field_name]?.reduce((acc, item) => {
                return acc === '' ? item.msg : [acc, item.msg].join('; ')
            }, '')
        },
        compareObjectsDiff(o1, o2={}, required_fields) {
            return Object.keys(o2).reduce((diff, key) => {
                if (o1[key] !== o2[key] || required_fields.includes(key)) {
                    return {
                        ...diff,
                        [key]: o2[key]
                    }
                } else {
                    return diff
                }
            }, {})
        },
        get_data() {

            const data = {
                common_params: {
                    name: this.name,
                    test_type: this.test_type,
                    env_type: this.env_type,
                    entrypoint: this.entrypoint,
                    runner: this.runner,
                    source: this.source.get(),
                    env_vars: {
                        cpu_quota: this.cpu_quota,
                        memory_quota: this.memory_quota,
                        cloud_settings: this.compareObjectsDiff(
                            this.$refs.locations.chosen_location_settings,
                            this.cloud_settings,
                            ["id", "integration_name", "instance_type"]
                        )
                    },
                    parallel_runners: this.parallel_runners,
                    cc_env_vars: {},
                    customization: this.customization,
                    location: this.location
                },
                test_parameters: this.test_parameters.get(),
                integrations: this.integrations?.get() || {},
                scheduling: this.scheduling?.get() || [],
            }
            let csv_files = {}
            $("#splitCSV .flex-row").slice(1,).each(function (_, item) {
                const file = $(item).find('input[type=text]')
                const header = $(item).find('input[type=checkbox]')
                if (file[0].value) {
                    csv_files[file[0].value] = header[0].checked
                }
            })
            if (Object.keys(csv_files).length > 0) {
                data.common_params.cc_env_vars.csv_files = csv_files
            }
            return data
        },
        handle_advanced_params_icon(e) {
            this.advanced_params_icon = this.$refs.advanced_params.classList.contains('show') ?
                'fas fa-chevron-down' : 'fas fa-chevron-up'
        },
        async handleCreate(run_test = false) {
            this.clearErrors()
            const resp = await fetch(`/api/v1/backend_performance/tests/${getSelectedProjectId()}`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({...this.get_data(), run_test})
            })
            if (resp.ok) {
                this.hide()
                vueVm.registered_components.table_tests?.table_action('refresh')
                run_test && vueVm.registered_components.table_results?.table_action('refresh')
            } else {
                await this.handleError(resp)
            }
        },
        async handleUpdate(run_test = false) {
            this.clearErrors()
            const resp = await fetch(`/api/v1/backend_performance/test/${getSelectedProjectId()}/${this.id}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({...this.get_data(), run_test})
            })
            if (resp.ok) {
                this.hide()
                vueVm.registered_components.table_tests?.table_action('refresh')
                run_test && vueVm.registered_components.table_results?.table_action('refresh')
            } else {
                await this.handleError(resp)
            }
        },
        async handleError(response) {
            try {
                const error_data = await response.json()
                this.errors = error_data?.reduce((acc, item) => {
                    const [errLoc, ...rest] = item.loc
                    item.loc = rest
                    if (acc[errLoc]) {
                        acc[errLoc].push(item)
                    } else {
                        acc[errLoc] = [item]
                    }
                    return acc
                }, {})

            } catch (e) {
                alertCreateTest.add(e, 'danger-overlay', true, 5000)
            }
        },
        initial_state() {
            return {
                id: null,
                uid: null,

                name: '',
                test_type: '',
                env_type: '',

                location: 'default',
                parallel_runners: 1,
                cpu_quota: 1,
                memory_quota: 4,
                cloud_settings: {},

                entrypoint: '',
                runner: this.default_runner,
                env_vars: {},
                customization: {},
                cc_env_vars: {},

                compile_tests: false,
                errors: {},

                advanced_params_icon: 'fas fa-chevron-down',
                mode: 'create',
                active_source_tab: undefined,
            }
        },
        set(data) {
            const {test_parameters, integrations, scheduling, source, env_vars: all_env_vars, ...rest} = data

            const {cpu_quota, memory_quota, cloud_settings, ...env_vars} = all_env_vars

            let test_type = ''
            let env_type = ''
            const test_parameters_filtered = test_parameters.filter(item => {
                if (item.name === 'test_type') {
                    test_type = item.default;
                    return false
                }
                if (item.name === 'env_type') {
                    env_type = item.default;
                    return false
                }
                return item.name !== 'test_name';

            })
            // common fields
            Object.assign(this.$data, {...rest, cpu_quota, memory_quota, cloud_settings, env_vars, test_type, env_type})

            // special fields
            this.test_parameters.set(test_parameters_filtered)
            this.source.set(source)
            integrations && this.integrations?.set(integrations)
            scheduling && this.scheduling.set(scheduling)

            rest?.customization && $(this.$refs.advanced_params).collapse('show')

            this.show()
        },
        clear() {
            Object.assign(this.$data, this.initial_state())
            this.test_parameters.clear()
            this.source.clear()
            this.integrations?.clear()
            this.scheduling.clear()
            $('#backend_parallel').text(this.parallel_runners)
            $('#backend_cpu').text(this.cpu_quota)
            $('#backend_memory').text(this.memory_quota)
            this.$refs.customization_component.clear()
        },
        clearErrors() {
            this.errors = {}
        },
        show() {
            $(this.$el).modal('show')
        },
        hide() {
            $(this.$el).modal('hide')
            // this.clear() // - happens on close event
        },
    }
}

register_component('TestCreateModal', TestCreateModal)


function addCSVSplit(id, key = "", is_header = "") {
    $(`#${id}`).append(`<div class="d-flex flex-row">
    <div class="flex-fill">
        <input type="text" class="form-control form-control-alternative" placeholder="File Path" value="${key}">
    </div>
    <div class="flex-fill m-auto pl-3">
        <div class="form-check">
          <input class="form-check-input" type="checkbox" value="">
          <label class="form-check-label">Ignore first line</label>
        </div>
    </div>
    <div class="m-auto">
        <button type="button" class="btn btn-32 btn-action" onclick="removeParam(event)"><i class="fas fa-minus"></i></button>
    </div>
</div>`)
}

// function toggleRows(id) {
//     $(`#${id}`).append(`<div class="d-flex flex-row">
//     <div class="flex-fill">
//         <input type="text" class="form-control form-control-alternative" placeholder="Failed thresholds rate" value="20">
//         <label class="form-check-label">Failed thresholds rate. If the failed thresholds rate in the test is higher than this number, the test will be considered as failed</label>
//     </div>
//
// </div>`)
// }


const TestRunModal = {
    delimiters: ['[[', ']]'],
    props: ['test_params_id', 'instance_name_prefix'],
    template: `
        <div class="modal modal-base fixed-left fade shadow-sm" tabindex="-1" role="dialog" id="runTestModal">
            <div class="modal-dialog modal-dialog-aside" role="document">
                <div class="modal-content">
                    <div class="modal-header">
                        <div class="row w-100">
                            <div class="col">
                                <h2>Run Backend Test</h2>
                            </div>
                            <div class="col-xs">
                                <button type="button" class="btn btn-secondary mr-2" data-dismiss="modal" aria-label="Close">
                                    Cancel
                                </button>
                                <button type="button" class="btn btn-basic" 
                                    @click="handleRun"
                                >
                                    Run test
                                </button>
                            </div>
                        </div>
                    </div>
                    <div class="modal-body">
                        <slot name="test_parameters"></slot>
                        <Locations 
                            v-model:location="location"
                            v-model:parallel_runners="parallel_runners"
                            v-model:cpu="cpu_quota"
                            v-model:memory="memory_quota"
                            v-model:cloud_settings="cloud_settings"
                            
                            ref="locations"
                        ></Locations>
                        <slot name="integrations"></slot>
                    </div>
                </div>
            </div>
        </div>
    `,
    computed: {
        test_parameters() {
            return ParamsTable.Manager(this.$props.test_params_id)
        },
        integrations() {
            try {
                return IntegrationSection.Manager(this.$props.instance_name_prefix)
            } catch (e) {
                console.warn('No integration section')
                return undefined
            }
        },
    },
    mounted() {
        $(this.$el).on('hide.bs.modal', this.clear)
    },
    data() {
        return this.initial_state()
    },
    methods: {
        initial_state() {
            return {
                id: null,
                uid: null,

                location: 'default',
                parallel_runners: 1,
                cpu_quota: 1,
                memory_quota: 4,
                cloud_settings: {},

                env_vars: {},
                customization: {},
                cc_env_vars: {},

                compile_tests: false,
                errors: {},
            }
        },
        set(data) {
            console.log('set data called', data)
            const {test_parameters, env_vars: all_env_vars, integrations, ...rest} = data

            const {cpu_quota, memory_quota, cloud_settings, ...env_vars} = all_env_vars

            // common fields
            Object.assign(this.$data, {...rest, cpu_quota, memory_quota, cloud_settings, env_vars,})

            // special fields
            this.test_parameters.set(test_parameters)

            this.integrations?.set(integrations)
            this.show()
        },
        show() {
            $(this.$el).modal('show')
        },
        hide() {
            $(this.$el).modal('hide')
            // this.clear() // - happens on close event
        },
        clear() {
            Object.assign(this.$data, this.initial_state())
            this.test_parameters.clear()
            this.integrations?.clear()
        },
        clearErrors() {
            this.errors = {}
        },
        get_data() {
            const test_params = this.test_parameters.get()
            const integrations = this.integrations?.get()
            const name = test_params.find(i => i.name === 'test_name')
            const test_type = test_params.find(i => i.name === 'test_type')
            const env_type = test_params.find(i => i.name === 'env_type')

            return {
                common_params: {
                    name: name,
                    test_type: test_type,
                    env_type: env_type,
                    env_vars: {
                        cpu_quota: this.cpu_quota,
                        memory_quota: this.memory_quota,
                        cloud_settings: this.cloud_settings
                    },
                    parallel_runners: this.parallel_runners,
                    location: this.location
                },
                test_parameters: test_params,
                integrations: integrations,
            }
        },
        async handleRun() {
            this.clearErrors()
            const resp = await fetch(`/api/v1/backend_performance/test/${getSelectedProjectId()}/${this.id}`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(this.get_data())
            })
            if (resp.ok) {
                this.hide()
                // vueVm.registered_components.table_tests?.table_action('refresh')
                vueVm.registered_components.table_results?.table_action('refresh')
            } else {
                await this.handleError(resp)
            }
        },
        async handleError(response) {
            try {
                const error_data = await response.json()
                this.errors = error_data?.reduce((acc, item) => {
                    const [errLoc, ...rest] = item.loc
                    item.loc = rest
                    if (acc[errLoc]) {
                        acc[errLoc].push(item)
                    } else {
                        acc[errLoc] = [item]
                    }
                    return acc
                }, {})

            } catch (e) {
                alertCreateTest.add(e, 'danger-overlay', true, 5000)
            }
        },
    },
    watch: {
        errors(newValue,) {
            if (Object.keys(newValue).length > 0) {
                newValue.test_parameters ?
                    this.test_parameters.setError(newValue.test_parameters) :
                    this.test_parameters.clearErrors()
                newValue.integrations ?
                    this.integrations?.setError(newValue.integrations) :
                    this.integrations?.clearErrors()
            } else {
                this.test_parameters.clearErrors()
                this.integrations?.clearErrors()
            }
        }
    },
}
register_component('TestRunModal', TestRunModal)

const test_delete = ids => {
    const url = `/api/v1/backend_performance/tests/${getSelectedProjectId()}?` + $.param({"id[]": ids})
    fetch(url, {
        method: 'DELETE'
    }).then(response => response.ok && vueVm.registered_components.table_tests?.table_action('refresh'))
}

const results_delete = ids => {
    const url = `/api/v1/backend_performance/reports/${getSelectedProjectId()}?` + $.param({"id[]": ids})
    fetch(url, {
        method: 'DELETE'
    }).then(response => response.ok && vueVm.registered_components.table_results?.table_action('refresh'))
}

$(document).on('vue_init', () => {
    $('#delete_tests').on('click', e => {
        const ids_to_delete = vueVm.registered_components.table_tests?.table_action('getSelections').map(
            item => item.id
        ).join(',')
        ids_to_delete && test_delete(ids_to_delete)
    })
    $('#delete_results').on('click', e => {
        const ids_to_delete = vueVm.registered_components.table_results?.table_action('getSelections').map(
            item => item.id
        ).join(',')
        ids_to_delete && results_delete(ids_to_delete)
    })
})
