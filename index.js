const AthenaQuery = require('./athenaQuery');
const async = require('async');

async.waterfall([
    (callback) => {
        console.log("CALLBACK 1", callback);
        AthenaQuery.createQueryExecutionId(callback);
    },
    (query, callback) => {
        async.retry(100000, AthenaQuery.checkQueryCreateStatus.bind(query), (err, result) => {
            console.log("CHECKING QUERY STATUS", err, result);
            if (!err) {
                console.log("SUCCESS");
                callback(null, query)
            }
            else if (err) {
                callback(err)
            } else {
                console.log("NOT SURE WHAT TO DO");
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