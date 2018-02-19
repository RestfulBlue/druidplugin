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
System.register(["moment", "angular", "lodash", "app/core/utils/datemath"], function(exports_1) {
    var moment_1, angular_1, lodash_1, dateMath;
    var DruidDatasource;
    return {
        setters:[
            function (moment_1_1) {
                moment_1 = moment_1_1;
            },
            function (angular_1_1) {
                angular_1 = angular_1_1;
            },
            function (lodash_1_1) {
                lodash_1 = lodash_1_1;
            },
            function (dateMath_1) {
                dateMath = dateMath_1;
            }],
        execute: function() {
            DruidDatasource = (function () {
                function DruidDatasource(instanceSettings, $q, backendSrv, templateSrv) {
                    this.GRANULARITIES = [
                        ['second', moment_1["default"].duration(1, 'second')],
                        ['minute', moment_1["default"].duration(1, 'minute')],
                        ['fifteen_minute', moment_1["default"].duration(15, 'minute')],
                        ['thirty_minute', moment_1["default"].duration(30, 'minute')],
                        ['hour', moment_1["default"].duration(1, 'hour')],
                        ['day', moment_1["default"].duration(1, 'day')],
                        ['none', moment_1["default"].duration(1, 'milliseconds')]
                    ];
                    this.filterTemplateExpanders = {
                        "selector": lodash_1["default"].partialRight(this.replaceTemplateValues, ['value']),
                        "regex": lodash_1["default"].partialRight(this.replaceTemplateValues, ['pattern']),
                        "javascript": lodash_1["default"].partialRight(this.replaceTemplateValues, ['function']),
                        "search": lodash_1["default"].partialRight(this.replaceTemplateValues, [])
                    };
                    this.type = 'druid-datasource';
                    this.url = instanceSettings.url;
                    this.name = instanceSettings.name;
                    this.basicAuth = instanceSettings.basicAuth;
                    instanceSettings.jsonData = instanceSettings.jsonData || {};
                    this.supportMetrics = true;
                    this.periodGranularity = instanceSettings.jsonData.periodGranularity;
                    this.templateSrv = templateSrv;
                    this.backendSrv = backendSrv;
                }
                DruidDatasource.prototype.replaceTemplateValues = function (obj, attrList) {
                    var _this = this;
                    var substitutedVals = attrList.map(function (attr) {
                        return _this.templateSrv.replace(obj[attr]);
                    });
                    return lodash_1["default"].assign(lodash_1["default"].clone(obj), lodash_1["default"].zipObject(attrList, substitutedVals));
                };
                DruidDatasource.prototype.testDatasource = function () {
                    return this._get('/druid/v2/datasources').then(function () {
                        return { status: "success", message: "Druid Data source is working", title: "Success" };
                    });
                };
                ;
                //Get list of available datasources
                DruidDatasource.prototype.getDataSources = function () {
                    return this._get('/druid/v2/datasources').then(function (response) {
                        return response.data;
                    });
                };
                ;
                DruidDatasource.prototype.getDimensionsAndMetrics = function (datasource) {
                    return this._get('/druid/v2/datasources/' + datasource).then(function (response) {
                        return response.data;
                    });
                };
                ;
                DruidDatasource.prototype.getFilterValues = function (target, panelRange, query) {
                    var topNquery = {
                        "queryType": "topN",
                        "dataSource": target.druidDS,
                        "granularity": 'all',
                        "threshold": 10,
                        "dimension": target.currentFilter.dimension,
                        "metric": "count",
                        "aggregations": [{ "type": "count", "name": "count" }],
                        "intervals": this.getQueryIntervals(panelRange.from, panelRange.to)
                    };
                    var filters = [];
                    if (target.filters) {
                        filters = angular_1["default"].copy(target.filters);
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
                ;
                DruidDatasource.prototype._get = function (relativeUrl, params) {
                    return this.backendSrv.datasourceRequest({
                        method: 'GET',
                        url: this.url + relativeUrl,
                        params: params
                    });
                };
                ;
                DruidDatasource.prototype.metricFindQuery = function (query) {
                    var _this = this;
                    var druidSqlQuery = {
                        query: query,
                        context: {
                            "sqlTimeZone": this.periodGranularity
                        }
                    };
                    return new Promise(function (resolve, reject) {
                        _this._druidQuery(druidSqlQuery, "/druid/v2/sql")
                            .then(function (result) {
                            var variableData = result.data
                                .map(function (row) {
                                var vals = [];
                                for (var property in row) {
                                    vals.push({ "text": row[property] });
                                }
                                return vals;
                            })
                                .reduce(function (a, b) {
                                return a.concat(b);
                            });
                            resolve(variableData);
                        }, function (error) {
                            console.log(error.data.errorMessage);
                            reject(new Error(error.data.errorMessage));
                        });
                    });
                };
                // Called once per panel (graph)
                DruidDatasource.prototype.query = function (options) {
                    var _this = this;
                    var dataSource = this;
                    var from = this.dateToMoment(options.range.from, false);
                    var to = this.dateToMoment(options.range.to, true);
                    var promises = options.targets.map(function (target) {
                        if (target.hide === true || lodash_1["default"].isEmpty(target.druidDS) || (lodash_1["default"].isEmpty(target.aggregators) && target.queryType !== "select")) {
                            console.log("target.hide: " + target.hide + ", target.druidDS: " + target.druidDS + ", target.aggregators: " + target.aggregators);
                            return Promise.resolve([]);
                        }
                        var maxDataPointsByResolution = options.maxDataPoints;
                        var maxDataPointsByConfig = target.maxDataPoints ? target.maxDataPoints : Number.MAX_VALUE;
                        var maxDataPoints = Math.min(maxDataPointsByResolution, maxDataPointsByConfig);
                        var granularity = target.shouldOverrideGranularity ? target.customGranularity : _this.computeGranularity(from, to, maxDataPoints);
                        //Round up to start of an interval
                        //Width of bar chars in Grafana is determined by size of the smallest interval
                        var roundedFrom = granularity === "all" ? from : _this.roundUpStartTime(from, granularity);
                        if (dataSource.periodGranularity != "") {
                            if (granularity === 'day') {
                                granularity = { "type": "period", "period": "P1D", "timeZone": dataSource.periodGranularity };
                            }
                        }
                        return dataSource._doQuery(roundedFrom, to, granularity, target);
                    });
                    return Promise.all(promises).then(function (results) {
                        return { data: lodash_1["default"].flatten(results) };
                    });
                };
                ;
                DruidDatasource.prototype._doQuery = function (from, to, granularity, target) {
                    var _this = this;
                    var datasource = target.druidDS;
                    var filters = target.filters;
                    var aggregators = target.aggregators;
                    var postAggregators = target.postAggregators;
                    var groupBy = lodash_1["default"].map(target.groupBy, function (e) {
                        return _this.templateSrv.replace(e);
                    });
                    var limitSpec = null;
                    var metricNames = this.getMetricNames(aggregators, postAggregators);
                    var intervals = this.getQueryIntervals(from, to);
                    var promise = null;
                    var skipEmptyBuckets = target.skipEmptyBuckets;
                    var selectMetrics = target.selectMetrics;
                    var selectDimensions = target.selectDimensions;
                    var selectThreshold = target.selectThreshold;
                    if (!selectThreshold) {
                        selectThreshold = 5;
                    }
                    if (target.queryType === 'topN') {
                        var threshold = target.limit;
                        var metric = target.druidMetric;
                        var dimension = this.templateSrv.replace(target.dimension);
                        promise = this._topNQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, threshold, metric, dimension)
                            .then(function (response) {
                            return _this.convertTopNData(response.data, dimension, metric);
                        });
                    }
                    else if (target.queryType === 'groupBy') {
                        limitSpec = this.getLimitSpec(target.limit, target.orderBy);
                        promise = this._groupByQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, groupBy, limitSpec)
                            .then(function (response) {
                            return _this.convertGroupByData(response.data, groupBy, metricNames);
                        });
                    }
                    else if (target.queryType === 'select') {
                        promise = this._selectQuery(datasource, intervals, granularity, selectDimensions, selectMetrics, filters, selectThreshold);
                        return promise.then(function (response) {
                            return _this.convertSelectData(response.data);
                        });
                    }
                    else {
                        promise = this._timeSeriesQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, skipEmptyBuckets)
                            .then(function (response) {
                            return _this.convertTimeSeriesData(response.data, metricNames);
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
                    return promise.then(function (metrics) {
                        var fromMs = _this.formatTimestamp(from);
                        metrics.forEach(function (metric) {
                            if (!lodash_1["default"].isEmpty(metric.datapoints[0]) && metric.datapoints[0][1] < fromMs) {
                                metric.datapoints[0][1] = fromMs;
                            }
                        });
                        return metrics;
                    });
                };
                ;
                DruidDatasource.prototype._selectQuery = function (datasource, intervals, granularity, dimension, metric, filters, selectThreshold) {
                    var query = {
                        "queryType": "select",
                        "dataSource": datasource,
                        "granularity": granularity,
                        "pagingSpec": { "pagingIdentifiers": {}, "threshold": selectThreshold },
                        "dimensions": dimension,
                        "metrics": metric,
                        "intervals": intervals
                    };
                    if (filters && filters.length > 0) {
                        query.filter = this.buildFilterTree(filters);
                    }
                    return this._druidQuery(query);
                };
                ;
                DruidDatasource.prototype._timeSeriesQuery = function (datasource, intervals, granularity, filters, aggregators, postAggregators, skipEmptyBuckets) {
                    var query = {
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
                        };
                    }
                    if (filters && filters.length > 0) {
                        query.filter = this.buildFilterTree(filters);
                    }
                    return this._druidQuery(query);
                };
                ;
                DruidDatasource.prototype._topNQuery = function (datasource, intervals, granularity, filters, aggregators, postAggregators, threshold, metric, dimension) {
                    var query = {
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
                ;
                DruidDatasource.prototype._groupByQuery = function (datasource, intervals, granularity, filters, aggregators, postAggregators, groupBy, limitSpec) {
                    var query = {
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
                ;
                DruidDatasource.prototype._druidQuery = function (query, path) {
                    if (path === void 0) { path = "/druid/v2/"; }
                    var options = {
                        method: 'POST',
                        url: this.url + path,
                        data: query
                    };
                    console.log("Make http request");
                    return this.backendSrv.datasourceRequest(options);
                };
                ;
                DruidDatasource.prototype.getLimitSpec = function (limitNum, orderBy) {
                    return {
                        "type": "default",
                        "limit": limitNum,
                        "columns": !orderBy ? null : orderBy.map(function (col) {
                            return { "dimension": col, "direction": "DESCENDING" };
                        })
                    };
                };
                DruidDatasource.prototype.buildFilterTree = function (filters) {
                    var _this = this;
                    //Do template letiable replacement
                    var replacedFilters = filters.map(function (filter) {
                        return _this.filterTemplateExpanders[filter.type](filter);
                    })
                        .map(function (filter) {
                        var finalFilter = lodash_1["default"].omit(filter, 'negate');
                        if (filter.negate) {
                            return { "type": "not", "field": finalFilter };
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
                };
                DruidDatasource.prototype.getQueryIntervals = function (from, to) {
                    return [from.toISOString() + '/' + to.toISOString()];
                };
                DruidDatasource.prototype.getMetricNames = function (aggregators, postAggregators) {
                    var displayAggs = lodash_1["default"].filter(aggregators, function (agg) {
                        return agg.type !== 'approxHistogramFold' && agg.hidden != true;
                    });
                    return lodash_1["default"].union(lodash_1["default"].map(displayAggs, 'name'), lodash_1["default"].map(postAggregators, 'name'));
                };
                DruidDatasource.prototype.formatTimestamp = function (ts) {
                    return moment_1["default"](ts).format('X') * 1000;
                };
                DruidDatasource.prototype.convertTimeSeriesData = function (md, metrics) {
                    var _this = this;
                    return metrics.map(function (metric) {
                        return {
                            target: metric,
                            datapoints: md.map(function (item) {
                                return [
                                    item.result[metric],
                                    _this.formatTimestamp(item.timestamp)
                                ];
                            })
                        };
                    });
                };
                DruidDatasource.prototype.getGroupName = function (groupBy, metric) {
                    return groupBy.map(function (dim) {
                        return metric.event[dim];
                    })
                        .join("-");
                };
                DruidDatasource.prototype.convertTopNData = function (md, dimension, metric) {
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
                    var _this = this;
                    /*
                      First, we need make sure that the result for each
                      timestamp contains entries for all distinct dimension values
                      in the entire list of results.
            
                      Otherwise, if we do a stacked bar chart, Grafana doesn't sum
                      the metrics correctly.
                    */
                    //Get the list of all distinct dimension values for the entire result set
                    var dVals = md.reduce(function (dValsSoFar, tsItem) {
                        var dValsForTs = lodash_1["default"].map(tsItem.result, dimension);
                        return lodash_1["default"].union(dValsSoFar, dValsForTs);
                    }, {});
                    //Add null for the metric for any missing dimension values per timestamp result
                    md.forEach(function (tsItem) {
                        var dValsPresent = lodash_1["default"].map(tsItem.result, dimension);
                        var dValsMissing = lodash_1["default"].difference(dVals, dValsPresent);
                        dValsMissing.forEach(function (dVal) {
                            var nullPoint = {};
                            nullPoint[dimension] = dVal;
                            nullPoint[metric] = null;
                            tsItem.result.push(nullPoint);
                        });
                        return tsItem;
                    });
                    //Re-index the results by dimension value instead of time interval
                    var mergedData = md.map(function (item) {
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
                        var timestamp = _this.formatTimestamp(item.timestamp);
                        var keys = lodash_1["default"].map(item.result, dimension);
                        var vals = lodash_1["default"].map(item.result, metric).map(function (val) {
                            return [val, timestamp];
                        });
                        return lodash_1["default"].zipObject(keys, vals);
                    })
                        .reduce(function (prev, curr) {
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
                        return lodash_1["default"].assignWith(prev, curr, function (pVal, cVal) {
                            if (pVal) {
                                pVal.push(cVal);
                                return pVal;
                            }
                            return [cVal];
                        });
                    }, {});
                    //Convert object keyed by dimension values into an array
                    //of objects {target: <dimVal>, datapoints: <metric time series>}
                    return lodash_1["default"].map(mergedData, function (vals, key) {
                        return {
                            target: key,
                            datapoints: vals
                        };
                    });
                };
                DruidDatasource.prototype.convertGroupByData = function (md, groupBy, metrics) {
                    var _this = this;
                    var mergedData = md.map(function (item) {
                        /*
                          The first map() transforms the list Druid events into a list of objects
                          with keys of the form "<groupName>:<metric>" and values
                          of the form [metricValue, unixTime]
                        */
                        var groupName = _this.getGroupName(groupBy, item);
                        var keys = metrics.map(function (metric) {
                            return groupName + ":" + metric;
                        });
                        var vals = metrics.map(function (metric) {
                            return [
                                item.event[metric],
                                _this.formatTimestamp(item.timestamp)
                            ];
                        });
                        return lodash_1["default"].zipObject(keys, vals);
                    })
                        .reduce(function (prev, curr) {
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
                        return lodash_1["default"].assignWith(prev, curr, function (pVal, cVal) {
                            if (pVal) {
                                pVal.push(cVal);
                                return pVal;
                            }
                            return [cVal];
                        });
                    }, {});
                    return lodash_1["default"].map(mergedData, function (vals, key) {
                        /*
                          Second map converts the aggregated object into an array
                        */
                        return {
                            target: key,
                            datapoints: vals
                        };
                    });
                };
                DruidDatasource.prototype.convertSelectData = function (data) {
                    var resultList = lodash_1["default"].map(data, "result");
                    var eventsList = lodash_1["default"].map(resultList, "events");
                    var eventList = lodash_1["default"].flatten(eventsList);
                    var result = {};
                    for (var i = 0; i < eventList.length; i++) {
                        var event_1 = eventList[i].event;
                        var timestamp = event_1.timestamp;
                        if (lodash_1["default"].isEmpty(timestamp)) {
                            continue;
                        }
                        for (var key in event_1) {
                            if (key !== "timestamp") {
                                if (!result[key]) {
                                    result[key] = { "target": key, "datapoints": [] };
                                }
                                result[key].datapoints.push([event_1[key], timestamp]);
                            }
                        }
                    }
                    return lodash_1["default"].values(result);
                };
                DruidDatasource.prototype.dateToMoment = function (date, roundUp) {
                    if (date === 'now') {
                        return moment_1["default"]();
                    }
                    date = dateMath.parse(date, roundUp);
                    return moment_1["default"](date.valueOf());
                };
                DruidDatasource.prototype.computeGranularity = function (from, to, maxDataPoints) {
                    var intervalSecs = to.unix() - from.unix();
                    /*
                      Find the smallest granularity for which there
                      will be fewer than maxDataPoints
                    */
                    var granularityEntry = lodash_1["default"].find(this.GRANULARITIES, function (gEntry) {
                        return Math.ceil(intervalSecs / gEntry[1].asSeconds()) <= maxDataPoints;
                    });
                    console.log("Calculated \"" + granularityEntry[0] + "\" granularity [" + Math.ceil(intervalSecs / granularityEntry[1].asSeconds()) +
                        " pts]" + " for " + (intervalSecs / 60).toFixed(0) + " minutes and max of " + maxDataPoints + " data points");
                    return granularityEntry[0];
                };
                DruidDatasource.prototype.roundUpStartTime = function (from, granularity) {
                    var duration = lodash_1["default"].find(this.GRANULARITIES, function (gEntry) {
                        return gEntry[0] === granularity;
                    })[1];
                    var rounded = null;
                    if (granularity === 'day') {
                        rounded = moment_1["default"](+from).startOf('day');
                    }
                    else {
                        rounded = moment_1["default"](Math.ceil((+from) / (+duration)) * (+duration));
                    }
                    console.log("Rounding up start time from " + from.format() + " to " + rounded.format() + " for granularity [" + granularity + "]");
                    return rounded;
                };
                return DruidDatasource;
            })();
            exports_1("DruidDatasource", DruidDatasource);
        }
    }
});
//# sourceMappingURL=datasource.js.map