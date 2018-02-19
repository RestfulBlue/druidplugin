/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import moment from "moment";
import angular from "angular";
import _ from "lodash";
import * as dateMath from "app/core/utils/datemath"

export class DruidDatasource {

    type: any;
    url: any;
    name: any;
    basicAuth: any;
    supportMetrics: boolean;
    periodGranularity: any;
    templateSrv: any;
    backendSrv: any;

    constructor(instanceSettings, $q, backendSrv, templateSrv) {
        this.type = 'druid-datasource';
        this.url = instanceSettings.url;
        this.name = instanceSettings.name;
        this.basicAuth = instanceSettings.basicAuth;
        instanceSettings.jsonData = instanceSettings.jsonData || {};
        this.supportMetrics = true;
        this.periodGranularity = instanceSettings.jsonData.periodGranularity;
        this.templateSrv = templateSrv;
        this.backendSrv = backendSrv
    }

    replaceTemplateValues(obj, attrList) {
        let substitutedVals = attrList.map((attr) => {
            return this.templateSrv.replace(obj[attr]);
        });
        return _.assign(_.clone(obj), _.zipObject(attrList, substitutedVals));
    }

    GRANULARITIES = [
        ['second', moment.duration(1, 'second')],
        ['minute', moment.duration(1, 'minute')],
        ['fifteen_minute', moment.duration(15, 'minute')],
        ['thirty_minute', moment.duration(30, 'minute')],
        ['hour', moment.duration(1, 'hour')],
        ['day', moment.duration(1, 'day')],
        ['none', moment.duration(1, 'milliseconds')]
    ];

    filterTemplateExpanders = {
        "selector": _.partialRight(this.replaceTemplateValues, ['value']),
        "regex": _.partialRight(this.replaceTemplateValues, ['pattern']),
        "javascript": _.partialRight(this.replaceTemplateValues, ['function']),
        "search": _.partialRight(this.replaceTemplateValues, []),
    };

    testDatasource() {
        return this._get('/druid/v2/datasources').then(() => {
            return {status: "success", message: "Druid Data source is working", title: "Success"};
        });
    };

    //Get list of available datasources
    getDataSources() {
        return this._get('/druid/v2/datasources').then((response) => {
            return response.data;
        });
    };

    getDimensionsAndMetrics(datasource) {
        return this._get('/druid/v2/datasources/' + datasource).then((response) => {
            return response.data;
        });
    };

    getFilterValues(target, panelRange, query) {
        let topNquery: any = {
            "queryType": "topN",
            "dataSource": target.druidDS,
            "granularity": 'all',
            "threshold": 10,
            "dimension": target.currentFilter.dimension,
            "metric": "count",
            "aggregations": [{"type": "count", "name": "count"}],
            "intervals": this.getQueryIntervals(panelRange.from, panelRange.to)
        };

        let filters = [];
        if (target.filters) {
            filters = angular.copy(target.filters);
        }
        filters.push({
            "type": "search",
            "dimension": target.currentFilter.dimension,
            "query": {
                "type": "insensitive_contains",
                "value": query
            }
        });
        topNquery.filter = this.buildFilterTree(filters);

        return this._druidQuery(topNquery);
    };

    _get(relativeUrl, params?) {
        return this.backendSrv.datasourceRequest({
            method: 'GET',
            url: this.url + relativeUrl,
            params: params,
        });
    };


    metricFindQuery(query: any) {
        let druidSqlQuery = {
            query: query,
            context: {
                "sqlTimeZone": this.periodGranularity
            }
        }

        return new Promise((resolve, reject) => {
            this._druidQuery(druidSqlQuery, "/druid/v2/sql")
                .then(
                    result => {
                        let variableData =
                            result.data
                                .map(row => {
                                    let vals = []
                                    for (let property in row) {
                                        vals.push({"text": row[property]})
                                    }
                                    return vals;
                                })
                                .reduce((a, b) => {
                                    return a.concat(b)
                                })

                        resolve(variableData)
                    },
                    error => {
                        console.log(error.data.errorMessage)
                        reject(new Error(error.data.errorMessage))
                    }
                )
        })

    }

    // Called once per panel (graph)
    query(options) {
        let dataSource = this;
        let from = this.dateToMoment(options.range.from, false);
        let to = this.dateToMoment(options.range.to, true);

        let promises = options.targets.map((target) => {
            if (target.hide === true || _.isEmpty(target.druidDS) || (_.isEmpty(target.aggregators) && target.queryType !== "select")) {
                console.log("target.hide: " + target.hide + ", target.druidDS: " + target.druidDS + ", target.aggregators: " + target.aggregators);
                return Promise.resolve([]);
            }
            let maxDataPointsByResolution = options.maxDataPoints;
            let maxDataPointsByConfig = target.maxDataPoints ? target.maxDataPoints : Number.MAX_VALUE;
            let maxDataPoints = Math.min(maxDataPointsByResolution, maxDataPointsByConfig);
            let granularity = target.shouldOverrideGranularity ? target.customGranularity : this.computeGranularity(from, to, maxDataPoints);
            //Round up to start of an interval
            //Width of bar chars in Grafana is determined by size of the smallest interval
            let roundedFrom = granularity === "all" ? from : this.roundUpStartTime(from, granularity);
            if (dataSource.periodGranularity != "") {
                if (granularity === 'day') {
                    granularity = {"type": "period", "period": "P1D", "timeZone": dataSource.periodGranularity}
                }
            }
            return dataSource._doQuery(roundedFrom, to, granularity, target);
        });

        return Promise.all(promises).then((results) => {
            return {data: _.flatten(results)};
        });
    };

    _doQuery(from, to, granularity, target) {
        let datasource = target.druidDS;
        let filters = target.filters;
        let aggregators = target.aggregators;
        let postAggregators = target.postAggregators;
        let groupBy = _.map(target.groupBy, (e) => {
            return this.templateSrv.replace(e)
        });
        let limitSpec = null;
        let metricNames = this.getMetricNames(aggregators, postAggregators);
        let intervals = this.getQueryIntervals(from, to);
        let promise = null;
        let skipEmptyBuckets = target.skipEmptyBuckets;

        let selectMetrics = target.selectMetrics;
        let selectDimensions = target.selectDimensions;
        let selectThreshold = target.selectThreshold;
        if (!selectThreshold) {
            selectThreshold = 5;
        }

        if (target.queryType === 'topN') {
            let threshold = target.limit;
            let metric = target.druidMetric;
            let dimension = this.templateSrv.replace(target.dimension);
            promise = this._topNQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, threshold, metric, dimension)
                .then((response) => {
                    return this.convertTopNData(response.data, dimension, metric);
                });
        }
        else if (target.queryType === 'groupBy') {
            limitSpec = this.getLimitSpec(target.limit, target.orderBy);
            promise = this._groupByQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, groupBy, limitSpec)
                .then((response) => {
                    return this.convertGroupByData(response.data, groupBy, metricNames);
                });
        }
        else if (target.queryType === 'select') {
            promise = this._selectQuery(datasource, intervals, granularity, selectDimensions, selectMetrics, filters, selectThreshold);
            return promise.then((response) => {
                return this.convertSelectData(response.data);
            });
        }
        else {

            promise = this._timeSeriesQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, skipEmptyBuckets)
                .then((response) => {
                    return this.convertTimeSeriesData(response.data, metricNames);
                });
        }
        /*
          At this point the promise will return an list of time series of this form
        [
          {
            target: <metric name>,
            datapoints: [
              [<metric value>, <timestamp in ms>],
              ...
            ]
          },
          ...
        ]

        Druid calculates metrics based on the intervals specified in the query but returns a timestamp rounded down.
        We need to adjust the first timestamp in each time series
        */
        return promise.then((metrics) => {
            let fromMs = this.formatTimestamp(from);
            metrics.forEach((metric) => {
                if (!_.isEmpty(metric.datapoints[0]) && metric.datapoints[0][1] < fromMs) {
                    metric.datapoints[0][1] = fromMs;
                }
            });
            return metrics;
        });
    };

    _selectQuery(datasource, intervals, granularity, dimension, metric, filters, selectThreshold) {
        let query: any = {
            "queryType": "select",
            "dataSource": datasource,
            "granularity": granularity,
            "pagingSpec": {"pagingIdentifiers": {}, "threshold": selectThreshold},
            "dimensions": dimension,
            "metrics": metric,
            "intervals": intervals
        };

        if (filters && filters.length > 0) {
            query.filter = this.buildFilterTree(filters);
        }

        return this._druidQuery(query);
    };

    _timeSeriesQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, skipEmptyBuckets) {
        let query: any = {
            "queryType": "timeseries",
            "dataSource": datasource,
            "granularity": granularity,
            "aggregations": aggregators,
            "postAggregations": postAggregators,
            "intervals": intervals
        };

        if (skipEmptyBuckets) {
            query.context = {
                "skipEmptyBuckets": "true"
            }
        }

        if (filters && filters.length > 0) {
            query.filter = this.buildFilterTree(filters);
        }

        return this._druidQuery(query);
    };

    _topNQuery(datasource, intervals, granularity, filters, aggregators, postAggregators,
               threshold, metric, dimension) {
        let query: any = {
            "queryType": "topN",
            "dataSource": datasource,
            "granularity": granularity,
            "threshold": threshold,
            "dimension": dimension,
            "metric": metric,
            // "metric": {type: "inverted", metric: metric},
            "aggregations": aggregators,
            "postAggregations": postAggregators,
            "intervals": intervals
        };

        if (filters && filters.length > 0) {
            query.filter = this.buildFilterTree(filters);
        }

        return this._druidQuery(query);
    };

    _groupByQuery(datasource, intervals, granularity, filters, aggregators, postAggregators,
                  groupBy, limitSpec) {
        let query: any = {
            "queryType": "groupBy",
            "dataSource": datasource,
            "granularity": granularity,
            "dimensions": groupBy,
            "aggregations": aggregators,
            "postAggregations": postAggregators,
            "intervals": intervals,
            "limitSpec": limitSpec
        };

        if (filters && filters.length > 0) {
            query.filter = this.buildFilterTree(filters);
        }

        return this._druidQuery(query);
    };

    _druidQuery(query, path = "/druid/v2/") {
        let options = {
            method: 'POST',
            url: this.url + path,
            data: query
        };
        console.log("Make http request");
        return this.backendSrv.datasourceRequest(options);
    };

    getLimitSpec(limitNum, orderBy) {
        return {
            "type": "default",
            "limit": limitNum,
            "columns": !orderBy ? null : orderBy.map((col) => {
                return {"dimension": col, "direction": "DESCENDING"};
            })
        };
    }

    buildFilterTree(filters) {
        //Do template letiable replacement
        let replacedFilters = filters.map((filter) => {
            return this.filterTemplateExpanders[filter.type](filter);
        })
            .map((filter) => {
                let finalFilter = _.omit(filter, 'negate');
                if (filter.negate) {
                    return {"type": "not", "field": finalFilter};
                }
                return finalFilter;
            });
        if (replacedFilters) {
            if (replacedFilters.length === 1) {
                return replacedFilters[0];
            }
            return {
                "type": "and",
                "fields": replacedFilters
            };
        }
        return null;
    }

    getQueryIntervals(from, to) {
        return [from.toISOString() + '/' + to.toISOString()];
    }

    getMetricNames(aggregators, postAggregators) {
        let displayAggs = _.filter(aggregators, (agg) => {
            return agg.type !== 'approxHistogramFold' && agg.hidden != true;
        });
        return _.union(_.map(displayAggs, 'name'), _.map(postAggregators, 'name'));
    }

    formatTimestamp(ts) {
        return moment(ts).format('X') * 1000;
    }

    convertTimeSeriesData(md, metrics) {
        return metrics.map((metric) => {
            return {
                target: metric,
                datapoints: md.map((item) => {
                    return [
                        item.result[metric],
                        this.formatTimestamp(item.timestamp)
                    ];
                })
            };
        });
    }

    getGroupName(groupBy, metric) {
        return groupBy.map((dim) => {
            return metric.event[dim];
        })
            .join("-");
    }

    convertTopNData(md, dimension, metric) {
        /*
          Druid topN results look like this:
          [
            {
              "timestamp": "ts1",
              "result": [
                {"<dim>": d1, "<metric>": mv1},
                {"<dim>": d2, "<metric>": mv2}
              ]
            },
            {
              "timestamp": "ts2",
              "result": [
                {"<dim>": d1, "<metric>": mv3},
                {"<dim>": d2, "<metric>": mv4}
              ]
            },
            ...
          ]
        */

        /*
          First, we need make sure that the result for each
          timestamp contains entries for all distinct dimension values
          in the entire list of results.

          Otherwise, if we do a stacked bar chart, Grafana doesn't sum
          the metrics correctly.
        */

        //Get the list of all distinct dimension values for the entire result set
        let dVals = md.reduce((dValsSoFar, tsItem) => {
            let dValsForTs = _.map(tsItem.result, dimension);
            return _.union(dValsSoFar, dValsForTs);
        }, {});

        //Add null for the metric for any missing dimension values per timestamp result
        md.forEach((tsItem) => {
            let dValsPresent = _.map(tsItem.result, dimension);
            let dValsMissing = _.difference(dVals, dValsPresent);
            dValsMissing.forEach((dVal) => {
                let nullPoint = {};
                nullPoint[dimension] = dVal;
                nullPoint[metric] = null;
                tsItem.result.push(nullPoint);
            });
            return tsItem;
        });

        //Re-index the results by dimension value instead of time interval
        let mergedData = md.map((item) => {
            /*
              This first map() transforms this into a list of objects
              where the keys are dimension values
              and the values are [metricValue, unixTime] so that we get this:
                [
                  {
                    "d1": [mv1, ts1],
                    "d2": [mv2, ts1]
                  },
                  {
                    "d1": [mv3, ts2],
                    "d2": [mv4, ts2]
                  },
                  ...
                ]
            */
            let timestamp = this.formatTimestamp(item.timestamp);
            let keys = _.map(item.result, dimension);
            let vals = _.map(item.result, metric).map((val) => {
                return [val, timestamp];
            });
            return _.zipObject(keys, vals);
        })
            .reduce((prev, curr) => {
                /*
                  Reduce() collapses all of the mapped objects into a single
                  object.  The keys are dimension values
                  and the values are arrays of all the values for the same key.
                  The _.assign() function merges objects together and it's callback
                  gets invoked for every key,value pair in the source (2nd argument).
                  Since our initial value for reduce() is an empty object,
                  the _.assign() callback will get called for every new val
                  that we add to the final object.
                */
                return _.assignWith(prev, curr, (pVal, cVal) => {
                    if (pVal) {
                        pVal.push(cVal);
                        return pVal;
                    }
                    return [cVal];
                });
            }, {});

        //Convert object keyed by dimension values into an array
        //of objects {target: <dimVal>, datapoints: <metric time series>}
        return _.map(mergedData, (vals, key) => {
            return {
                target: key,
                datapoints: vals
            };
        });
    }

    convertGroupByData(md, groupBy, metrics) {
        let mergedData = md.map((item) => {
            /*
              The first map() transforms the list Druid events into a list of objects
              with keys of the form "<groupName>:<metric>" and values
              of the form [metricValue, unixTime]
            */
            let groupName = this.getGroupName(groupBy, item);
            let keys = metrics.map((metric) => {
                return groupName + ":" + metric;
            });
            let vals = metrics.map((metric) => {
                return [
                    item.event[metric],
                    this.formatTimestamp(item.timestamp)
                ];
            });
            return _.zipObject(keys, vals);
        })
            .reduce((prev, curr) => {
                /*
                  Reduce() collapses all of the mapped objects into a single
                  object.  The keys are still of the form "<groupName>:<metric>"
                  and the values are arrays of all the values for the same key.
                  The _.assign() function merges objects together and it's callback
                  gets invoked for every key,value pair in the source (2nd argument).
                  Since our initial value for reduce() is an empty object,
                  the _.assign() callback will get called for every new val
                  that we add to the final object.
                */
                return _.assignWith(prev, curr, (pVal, cVal) => {
                    if (pVal) {
                        pVal.push(cVal);
                        return pVal;
                    }
                    return [cVal];
                });
            }, {});

        return _.map(mergedData, (vals, key) => {
            /*
              Second map converts the aggregated object into an array
            */
            return {
                target: key,
                datapoints: vals
            };
        });
    }

    convertSelectData(data) {
        let resultList = _.map(data, "result");
        let eventsList = _.map(resultList, "events");
        let eventList = _.flatten(eventsList);
        let result = {};
        for (let i = 0; i < eventList.length; i++) {
            let event = eventList[i].event;
            let timestamp = event.timestamp;
            if (_.isEmpty(timestamp)) {
                continue;
            }
            for (let key in event) {
                if (key !== "timestamp") {
                    if (!result[key]) {
                        result[key] = {"target": key, "datapoints": []};
                    }
                    result[key].datapoints.push([event[key], timestamp]);
                }
            }
        }
        return _.values(result);
    }

    dateToMoment(date, roundUp) {
        if (date === 'now') {
            return moment();
        }
        date = dateMath.parse(date, roundUp);
        return moment(date.valueOf());
    }

    computeGranularity(from, to, maxDataPoints) {
        let intervalSecs = to.unix() - from.unix();
        /*
          Find the smallest granularity for which there
          will be fewer than maxDataPoints
        */
        let granularityEntry = _.find(this.GRANULARITIES, (gEntry) => {
            return Math.ceil(intervalSecs / gEntry[1].asSeconds()) <= maxDataPoints;
        });

        console.log("Calculated \"" + granularityEntry[0] + "\" granularity [" + Math.ceil(intervalSecs / granularityEntry[1].asSeconds()) +
            " pts]" + " for " + (intervalSecs / 60).toFixed(0) + " minutes and max of " + maxDataPoints + " data points");
        return granularityEntry[0];
    }

    roundUpStartTime(from, granularity) {
        let duration = _.find(this.GRANULARITIES, (gEntry) => {
            return gEntry[0] === granularity;
        })[1];
        let rounded = null;
        if (granularity === 'day') {
            rounded = moment(+from).startOf('day');
        } else {
            rounded = moment(Math.ceil((+from) / (+duration)) * (+duration));
        }
        console.log("Rounding up start time from " + from.format() + " to " + rounded.format() + " for granularity [" + granularity + "]");
        return rounded;
    }

    //changes druid end
}
