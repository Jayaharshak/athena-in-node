const AthenaQuery = require('./athenaQuery');
const async = require('async');

async.waterfall([
    (callback) => {
        AthenaQuery.createQueryExecutionId(callback);
    },
    (query, callback) => {
        async.retry({
            times: 60,
            interval: 10000
        }, AthenaQuery.checkQueryCreateStatus.bind(query), (err, result) => {
            console.log("CHECKING QUERY STATUS", err, result);
            if (!err) {
                callback(null, query)
            }
            else {
                callback(err)
            }
        });
    },
    (query, callback) => {

        AthenaQuery.getQueryResultByExecutionId(query.QueryExecutionId, (err, result) => {
            console.log("QUERY RESULTS BY EXEC.ID", err, result);
            callback(null, result, query)
        })
    },
    (queryResult, query, callback) => {
        AthenaQuery.stopQueryExecutionId(query.QueryExecutionId, (err, result) => {
            console.log("%%%%STOP CALL%%%%");
            callback(err, queryResult)
        });
    }
], (error, result) => {
    console.log("******result*******", error, result);
})