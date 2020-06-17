//const { bigQueryMapper } = require("./bigQueryMapper");
const Twitter = require('twitter');
require('dotenv').config();
const testMode = process.env.TEST === "1" ? true : false;

const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
// const moment = require('moment');
// moment().format();

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const bucket = storage.bucket('tanelis_auth');
const fileName = 'twitterConfig.json';
const file = bucket.file(fileName);

/*
Test commands:
node -e 'require("./index").testing()'
*/

console.log(`Test mode is on: ${testMode}`);

const downloadFile = async (file) => {
    return file.download();
}

const getConfig = async () => {
    // Some configs like the Twitter API keys and BigQuery related parameters are stored in GC Storage
    const configFile = await downloadFile(file);
    const config = JSON.parse(configFile.toString()); // could there be a more convenient way for this?
    return config;
}

exports.twitterListener = async (data) => {
    const config = await getConfig();
    console.log(`Configurations (this log should be removed: ${JSON.stringify(config)})`);

    // BigQuery configurations
    const dataset = bigquery.dataset(config.bigQuery.datasetId);
    const table = dataset.table(config.bigQuery.insertTable);

    console.log(`Configurations loaded from storage: dataset = "${config.bigQuery.datasetId}", insert table = "${config.bigQuery.insertTable}".`);

    // Set up the Twitter client
    const keListId = config.twitter.twitterListId; // id of the list that has all the screen names of the meps
    const client = new Twitter({
        consumer_key: config.twitter.consumerKey,
        consumer_secret: config.twitter.consumerSecret,
        access_token_key: config.twitter.accessTokenKey,
        access_token_secret: config.twitter.accessTokenSecret
    });



}

