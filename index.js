//const { bigQueryMapper } = require("./bigQueryMapper");
// const Twitter = require('twitter');
// require('dotenv').config();
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
// const moment = require('moment');
// moment().format();

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const bucket = storage.bucket('tanelis_auth');
const fileName = 'twitterConfig.json';
const file = bucket.file(fileName);

const downloadFile = async (file) => {
    return file.download();
}

const getConfig = async () => {
    // Some configs like the Twitter API keys are stored in GC Storage
    const configFile = await downloadFile(file);
    const config = JSON.parse(configFile.toString()); // could there be a more convenient way for this?
    return config;
}

exports.testing = async () => {
    const config = await getConfig();

    // BigQuery configurations
    const dataset = bigquery.dataset(config.bigQuery.datasetId);
    const table = dataset.table(config.bigQuery.insertTable);

    console.log(`Configurations loaded from storage: dataset = "${config.bigQuery.datasetId}", insert table = "${config.bigQuery.insertTable}".`);

}

