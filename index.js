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

console.log(`Test mode is on: ${testMode}`);

/*
This function is the main function that is called by the pub/sub trigger.
TODO: change so that the twitter list id (and possibly some other configurations come from the pubsub event)
*/
exports.twitterListener2 = async (event) => {
    let eventPayload = null;
    if (event) {
        // function could be updated to get more config parameters from the pubsub event
        // that way it would be possible to use the same function for multiple jobs
        eventPayload = JSON.parse(Buffer.from(event.data, 'base64').toString());
        console.log(`Payload of the triggering event: ${JSON.stringify(eventPayload)}`);
    }

    const config = await getConfig();
    console.log(`Configurations: ${JSON.stringify(config)})`);

    console.log(`Configurations loaded from storage: dataset = "${config.bigQuery.datasetId}", insert table = "${config.bigQuery.insertTable}".`);

    const screenNames = await getScreenNames(config);
    console.log(`Number of screen names found: ${screenNames.length}`);



    // run the function to fetc the latest tweets into BigQuery
    const fetchJob = await fetchAndStoreTweets(config, screenNames, eventPayload);
    //console.log(fetchJob);
}

const getConfig = async () => {
    // Some configs like the Twitter API keys and BigQuery related parameters are stored in GC Storage
    const bucket = storage.bucket('tanelis_auth');
    const fileName = 'twitterConfig.json';
    const file = bucket.file(fileName);

    const configFile = await file.download();
    const config = JSON.parse(configFile.toString());
    return config;
}

const getScreenNames = async (config) => {
    // returns the list of screen names that are followed

    const twitterClient = new Twitter({
        consumer_key: config.twitter.consumerKey,
        consumer_secret: config.twitter.consumerSecret,
        access_token_key: config.twitter.accessTokenKey,
        access_token_secret: config.twitter.accessTokenSecret
    });

    // Screen name list from the Eduskunta managed twitter list (this is a fallback for the main list)
    const twitterList = await twitterClient.get('lists/members', { list_id: config.twitter.twitterListId, count: 300 }); // twitter member list's id is defined in the config json
    const twitterScreenNames = twitterList.users.map((user) => {
        return user.screen_name;
    });

    // Get the similar list from BigQuery (self managed main list)
    const query = `SELECT DISTINCT screen_name FROM \`${config.bigQuery.screenNames}\` 
    WHERE screen_name IS NOT NULL AND active_term = TRUE`
    const bqRequest = await queryRequest(query, 1000);
    bqScreenNames = bqRequest.map((user) => {
        return user.screen_name;
    });

    // Merge the lists into one
    const screenNames = Array.from(new Set(twitterScreenNames.concat(bqScreenNames)));

    return screenNames;
}

async function fetchAndStoreTweets(config, screenNames, eventPayload) {
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

    //const bqLatestTweets = await getAlreadyInsertedTweets(config.bigQuery.latestTweetsTable);
    const latestTweetIds = await queryRequest(`SELECT * FROM \`${config.bigQuery.latestTweetsTable}\``, 1000);
    //console.log(`Latest already collected tweet ids fetched from BQ for each mep: ${JSON.stringify(latestTweetIds)}`)

    // determine how old tweets are accepted
    const startDate = moment('2019-10-07');
    const sinceDate = moment().subtract(180, "days");

    // Merge the followed screen names with their latest tweet ids
    let membersLatest = screenNames.map((sn) => {
        return {
            user_screen_name: sn,
            max_id: null
        }
    }).map(x => Object.assign(x, latestTweetIds.find(y => y.user_screen_name == x.user_screen_name)));

    // If in test mode only include first 5 members
    if (testMode) {
        membersLatest = membersLatest.slice(5);
    }

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

const queryRequest = async (query, maxResults) => {
    // latestTweetsTable is a view in bigQuery that returns the id of the latest already collected tweet for each of the user names
    // insert options, raw: true means that the same rows format is used as in the API documentation
    const options = {
        maxResults: maxResults,
    };

    const results = await bigquery.query(query, options);

    return results[0];
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

const flatten = function (arr, result = []) {
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