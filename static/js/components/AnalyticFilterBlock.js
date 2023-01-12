const AnalyticFilterBlock = {
    components: {
        'analytic-filter-dropdown': AnalyticFilterDropdown,
    },
    props: ['analyticsData', 'blockIndex'],
    data() {
        return {
            transactionItems: null,
            metricItems: null,
            selectedTransactions: [],
            selectedMetrics: [],
            loaded: false,
            loadingData: false,
        }
    },
    mounted() {
        this.transactionItems = Object.keys(this.analyticsData).filter(item => {
            if (item !== '' && item !== "All") return item
        });
        this.metricItems = Object.keys(this.analyticsData["All"])
        this.loaded = true
    },
    methods: {
        setTransactions(payload) {
            this.selectedTransactions = [ ...payload];
        },
        setMetrics(payload) {
            this.selectedMetrics = [ ...payload];
        },
        handlerSubmit() {
            let blockItems = [];
            this.selectedMetrics.forEach(metric => {
                blockItems.push(...this.selectedTransactions.map(transaction => `${transaction}_${metric}`));
            })
            if(filtersBlock.get(this.blockIndex)) {
                analyticsLine.data.datasets = analyticsLine.data.datasets
                    .filter(item => !filtersBlock.get(this.blockIndex).includes(item.label))
            }
            filtersBlock.set(this.blockIndex, blockItems)

            if (this.selectedMetrics.length && this.selectedTransactions.length) {
                const allRequests = []
                this.selectedMetrics.forEach(metric => {
                    allRequests.push(getDataForAnalysis(metric, this.selectedTransactions));
                })
                this.fetchChartData(allRequests)
            }
        },
        fetchChartData(allRequests) {
            this.loadingData = true;
            Promise.all(allRequests).then(data => {
                if(analyticsLine.data.labels.length === 0 || analyticsLine.data.labels.length !== data[0].labels.length) {
                    const firstDatasets = []
                    data.forEach(item => {
                        firstDatasets.push(...item.datasets);
                    })
                    analyticsData.datasets = firstDatasets;
                    analyticsData.labels = data[0].labels;
                    analyticsLine.update();
                } else {
                    data.forEach(item => {
                        analyticsLine.data.datasets.push(...item.datasets);
                    })
                    analyticsLine.update();
                }
                turnOnAllLine();
                document.getElementById('chartjs-custom-legend-analytic').innerHTML = analyticsLine.generateLegend();
            }).catch(error => {
                console.log('ERROR', error)
            }).finally(() => {
                this.loadingData = false;
            });
        }
    },
    template: `
        <div v-if="loaded">
            <p class="font-h5 font-bold mb-1 text-gray-800">Transactions</p>
            <analytic-filter-dropdown
                @select-items="setTransactions"
                :items-list="transactionItems"
            ></analytic-filter-dropdown>
            <p class="font-h5 font-bold my-1 text-gray-800">Metrics</p>
            <analytic-filter-dropdown
                @select-items="setMetrics"
                :items-list="metricItems"
            ></analytic-filter-dropdown>
            <div class="pt-3">
                <button class="btn btn-secondary"
                    :disabled="loadingData"
                    style="position: relative; padding-right: 24px"
                    @click="handlerSubmit">Apply
                    <i
                        v-if="loadingData"
                        class="spinner-loader" 
                        style="position: absolute; top: 8px; right: 5px"></i>
                </button>
            </div>
        </div>
    `
}

register_component('analytic-filter-block', AnalyticFilterBlock);
