export declare class DruidDatasource {
    type: any;
    url: any;
    name: any;
    basicAuth: any;
    supportMetrics: boolean;
    periodGranularity: any;
    templateSrv: any;
    backendSrv: any;
    constructor(instanceSettings: any, $q: any, backendSrv: any, templateSrv: any);
    replaceTemplateValues(obj: any, attrList: any): any;
    GRANULARITIES: any[][];
    filterTemplateExpanders: {
        "selector": any;
        "regex": any;
        "javascript": any;
        "search": any;
    };
    testDatasource(): any;
    getDataSources(): any;
    getDimensionsAndMetrics(datasource: any): any;
    getFilterValues(target: any, panelRange: any, query: any): any;
    _get(relativeUrl: any, params?: any): any;
    metricFindQuery(query: any): Promise<{}>;
    query(options: any): Promise<{
        data: any;
    }>;
    _doQuery(from: any, to: any, granularity: any, target: any): any;
    _selectQuery(datasource: any, intervals: any, granularity: any, dimension: any, metric: any, filters: any, selectThreshold: any): any;
    _timeSeriesQuery(datasource: any, intervals: any, granularity: any, filters: any, aggregators: any, postAggregators: any, skipEmptyBuckets: any): any;
    _topNQuery(datasource: any, intervals: any, granularity: any, filters: any, aggregators: any, postAggregators: any, threshold: any, metric: any, dimension: any): any;
    _groupByQuery(datasource: any, intervals: any, granularity: any, filters: any, aggregators: any, postAggregators: any, groupBy: any, limitSpec: any): any;
    _druidQuery(query: any, path?: string): any;
    getLimitSpec(limitNum: any, orderBy: any): {
        "type": string;
        "limit": any;
        "columns": any;
    };
    buildFilterTree(filters: any): any;
    getQueryIntervals(from: any, to: any): string[];
    getMetricNames(aggregators: any, postAggregators: any): any;
    formatTimestamp(ts: any): number;
    convertTimeSeriesData(md: any, metrics: any): any;
    getGroupName(groupBy: any, metric: any): any;
    convertTopNData(md: any, dimension: any, metric: any): any;
    convertGroupByData(md: any, groupBy: any, metrics: any): any;
    convertSelectData(data: any): any;
    dateToMoment(date: any, roundUp: any): any;
    computeGranularity(from: any, to: any, maxDataPoints: any): any;
    roundUpStartTime(from: any, granularity: any): any;
}
