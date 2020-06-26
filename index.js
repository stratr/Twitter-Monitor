const { bigQueryMapper } = require("./bigQueryMapper");
const Twitter = require('twitter');
require('dotenv').config();
const testMode = process.env.TEST === "1" ? true : false;

const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const moment = require('moment');
moment().format();

const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const bucket = storage.bucket('tanelis_auth');
const fileName = 'twitterConfig.json';
const file = bucket.file(fileName);

/*
Test commands:
node -e 'require("./index").testing()'

Deploy:
// gcloud functions deploy twitterListener2 --runtime nodejs10 --trigger-topic fetch_ke_tweets2 --timeout 180s
*/

console.log(`Test mode is on: ${testMode}`);


/*
This function is the main function that is called by the pub/sub trigger.
TODO: change so that the twitter list id (and possibly some other configurations come from the pubsub event)
*/
exports.twitterListener2 = async (event) => {
    let eventPayload = null;
    if (event) {
        eventPayload = Buffer.from(event.data, 'base64').toString();
        console.log(`Payload of the triggering event: ${eventPayload}`);
    }

    const config = await getConfig();
    //console.log(`Configurations: ${JSON.stringify(config)})`);

    console.log(`Configurations loaded from storage: dataset = "${config.bigQuery.datasetId}", insert table = "${config.bigQuery.insertTable}".`);

    // run the function to fetc the latest tweets into BigQuery
    const fetchJob = await fetchAndStoreTweets(config, eventPayload);
    console.log(fetchJob);
}

const getConfig = async () => {
    // Some configs like the Twitter API keys and BigQuery related parameters are stored in GC Storage
    const configFile = await file.download();
    const config = JSON.parse(configFile.toString());
    return config;
}

async function fetchAndStoreTweets(config, eventPayload) {
    // Set up the Twitter client
    const client = new Twitter({
        consumer_key: config.twitter.consumerKey,
        consumer_secret: config.twitter.consumerSecret,
        access_token_key: config.twitter.accessTokenKey,
        access_token_secret: config.twitter.accessTokenSecret
    });

    // BigQuery configurations
    const dataset = bigquery.dataset(config.bigQuery.datasetId);
    const table = dataset.table(config.bigQuery.insertTable);

    const bqLatestTweets = await getAlreadyInsertedTweets(config.bigQuery.latestTweetsTable);
    const latestTweetIds = bqLatestTweets[0];
    //console.log(`Latest already collected tweet ids fetched from BQ for each mep: ${JSON.stringify(latestTweetIds)}`)

    // determine how old tweets are accepted
    const startDate = moment('2019-10-07');
    const sinceDate = moment().subtract(180, "days");

    // Get all the Twitter user names to fetch
    const members = await client.get('lists/members', { list_id: config.twitter.twitterListId, count: 300 }); // twitter member list's id is defined in the config json
    var memberScreenNames = members.users.map(member => { return { 'user_screen_name': member.screen_name, max_id: null } });
    if (typeof testMode !== 'undefined' && testMode === true) { memberScreenNames = memberScreenNames.slice(0, 5) }; // reduce the amount of user ids when testing
    console.log(`Count of member screen names to fetch: ${memberScreenNames.length}`)

    // Merge member screen names with their latest tweets
    const membersLatest = memberScreenNames.map(x => Object.assign(x, latestTweetIds.find(y => y.user_screen_name == x.user_screen_name)));

    // Get the tweets for the members fetched earlier
    const tweets = await fetchMultipleUserTweets(client, membersLatest);
    //console.log(`${tweets.length} tweets fetched.\nSome tweets:\n${tweets.slice(0,5).map(tweet => {return tweet.text ? tweet.text : tweet.full_text}).join('\n')}`)

    // Map the rows for the BQ streaming insert
    const bqRows = bigQueryMapper(tweets);
    const rowsFiltered = bqRows.filter(row => {
        return moment(row.json.date).isAfter(startDate) && moment(row.json.date).isAfter(sinceDate);
    });

    // Store the rows into BigQuery
    if (rowsFiltered.length > 0) {
        console.log(`Inserting ${rowsFiltered.length} rows.`);
        console.log(`Example row:\n${JSON.stringify(rowsFiltered[0])}`)

        const bqPageSize = 500; // rows per one page in the streaming insert
        const bqPages = arrayPages(rowsFiltered, bqPageSize);
        console.log(`Number of pages in the BQ streaming insert: ${bqPages}`);

        const bqPromises = [];
        for (let i = 0; i < bqPages; i++) {
            console.log('Paginating the insert request. Page number ' + i);
            var page = paginate(rowsFiltered, bqPageSize, i);
            if (page.length > 0) {
                const bqPromise = insertRowsAsStream(page, table);
                bqPromises.push(bqPromise);
            }
        }

        return Promise.all(bqPromises)
        .then(responses => {
            console.log('Function initiation data: ' + eventPayload);
            console.log('Responses: ' + responses.length);
            if (responses.length > 0) {
                console.log('Tweets inserted to BigQuery.');
            } else {
                console.log('No response from the streaming insert.')
            }

            return 'Process completed';

        })
        .catch((err) => {
            // An API error or partial failure occurred.
            console.log(JSON.stringify(err));

            if (err.name === 'PartialFailureError') {
                console.log('PartialFailureError');
            }
        });
    } else {
        console.log(`No new tweets found. Aborting.`);
        return 'No tweets to fetch.';
    }
}

function getAlreadyInsertedTweets(latestTweetsTable) {
    // latestTweetsTable is a view in bigQuery that returns the id of the latest already collected tweet for each of the user names
    // insert options, raw: true means that the same rows format is used as in the API documentation
    const options = {
        maxResults: 1000,
    };

    const query = "SELECT * FROM " + latestTweetsTable;

    console.log(query);

    return bigquery.query(query, options);
}

async function fetchMultipleUserTweets(client, members) {
    const tweetPromises = [];
    members.forEach(member => {
        const sinceId = member.max_id === null ? '1' : member.max_id;

        // Fetch all the tweets, for eachof the users, that have not yet been collected
        const tweetPromise = client.get('statuses/user_timeline', {
            screen_name: member.user_screen_name,
            count: 200,
            include_rts: true,
            since_id: sinceId,
            tweet_mode: 'extended'
        });
        tweetPromises.push(tweetPromise);
    });

    return Promise.all(tweetPromises)
        .then(tweets => {
            return flatten(tweets);
        })
        .catch(err => {
            console.log('Error in fetching tweets.');
            console.log(err);
        });
}

const flatten = function(arr, result = []) {
    for (let i = 0, length = arr.length; i < length; i++) {
        const value = arr[i];
        if (Array.isArray(value)) {
            flatten(value, result);
        } else {
            result.push(value);
        }
    }
    return result;
};

function paginate(array, page_size, page_number) {
    return array.slice(page_number * page_size, (page_number + 1) * page_size);
}

function arrayPages(array, pageSize) {
    return Math.ceil(array.length / pageSize);
}

function insertRowsAsStream(rows, table) {
    // insert options, raw: true means that the same rows format is used as in the API documentation
    const options = {
        raw: true,
        allowDuplicates: false
    };

    return table.insert(rows, options);
}