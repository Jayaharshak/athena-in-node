const AWS = require('aws-sdk');
// const credentials = new AWS.SharedIniFileCredentials({profile: 'default'});
// AWS.config.credentials = credentials;
const awsCredentials = {
    region: "us-east-1"
};

AWS.config.update(awsCredentials);
const athena = new AWS.Athena({
    region: 'us-east-1'
});

/**
 * @description createQueryExecutionId, execute for generating queryExecutionId
 * @param {Function} callback
 */
function createQueryExecutionId(callback){
    console.log("******createQueryExecutionId****");
    /**doing resultConfiguration, but we will not save query result there. */
    const params = {
        QueryString: 'SELECT * FROM firststream.tb_first_stream limit 10;', /* required */
        ResultConfiguration: { /* required */
            OutputLocation: `s3://ahtena-query-results/`, /* required */
            EncryptionConfiguration: {
                EncryptionOption: 'SSE_S3', /* required */
            }
        }
    };
    athena.startQueryExecution(params, function(err, data) {
        console.log("***Query exec Stage*****", err, data);
        callback(err?err.stack:err, data)
    });
}
/**
 * @description checkQueryCreateStatus, check query status till it is not active.
 */
function checkQueryCreateStatus(callback){
    const params = {
        QueryExecutionId: this.QueryExecutionId /* required */
    };
    athena.getQueryExecution(params, function(err, data) {
        if (err) console.log("ERRRRRRR",err, err.stack); // an error occurred
        else{
            if(data && data.QueryExecution && data.QueryExecution.Status && data.QueryExecution.Status.State && data.QueryExecution.Status.State === ('RUNNING' || "QUEUED")){
                console.log("Athena Query status is running");
                callback("RUNNING");
            }
            else{
                console.log("Atehna Query status is Active")
                callback(err?err.stack:err, data);
            }
        }
    });
}
/**
 * @description getQueryResultByExecutionId, execute for generating result based on queryExecutionId
 * @param {String} queryExecutionId
 * @param {Function} callback
 */
function getQueryResultByExecutionId(queryExecutionId, callback){
    const params = {
        QueryExecutionId: queryExecutionId
    };
    athena.getQueryResults(params, function(err, data) {
        // console.log(err, data)
        callback(err?err.stack:err, data) 
    });
}

/**
 * @description stopQueryExecutionId, execute for stop queryExecutionId
 * @param {String} queryExecutionId
 * @param {Function} callback
 */
function stopQueryExecutionId(queryExecutionId, callback){
    const params = {
        QueryExecutionId: queryExecutionId
    };
    athena.stopQueryExecution(params, function(err, data) {
        callback(err?err.stack:err, data) 
    });
}

module.exports = {
    createQueryExecutionId: createQueryExecutionId,
    checkQueryCreateStatus: checkQueryCreateStatus,
    getQueryResultByExecutionId: getQueryResultByExecutionId,
    stopQueryExecutionId: stopQueryExecutionId
}
