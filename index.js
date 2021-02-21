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

    // TODO: use something like this to pass data from the pub/sub event to the function
    // const triggerData = {
    //     runMode: 'test',
    //     insertDataSet: '',
    //     insertTable: ''        
    // }

    // Load the configuration file from Storage (TODO maybe use env variables or event parameters instead)
    const config = await getConfig();
    console.log(`Configurations: ${JSON.stringify(config)})`);

    // Get the latest tweet id for all of the mps that have at least some tweet stored in the DB
    const latestTweetIds = await queryRequest(`SELECT * FROM \`${config.bigQuery.latestTweetsTable}\``, 1000);
    console.log(`Found latest tweet id for ${latestTweetIds.length} members. Can include also members who are not anymore on the list of followed users.`);

    // Get the list of screen names for the users that are being followed
    const screenNames = await getScreenNames(config);
    console.log(`Number of screen names found: ${screenNames.length}`);

    // Merge the followed screen names with their latest tweet ids
    let membersLatest = screenNames.map((sn) => {
        return {
            user_screen_name: sn,
            max_id: null
        }
    }).map(x => Object.assign(x, latestTweetIds.find(y => y.user_screen_name == x.user_screen_name)));

    // If in test mode only include first 5 members
    if (testMode) {
        console.log('Running in test mode: limited twitter API requests to user timelines.')
        membersLatest = membersLatest.slice(170, 180);
        console.log(`membersLatest.length = ${membersLatest.length}`)
    }

    // Check the timelines for each of the followed members and fetch the latest tweets
    const tweets = await fetchMultipleUserTweets(config, membersLatest);

    // Filter the tweets with this date criteria
    const startDate = moment('2019-10-07');
    const sinceDate = moment().subtract(180, "days");
    const tweetsFiltered = tweets.filter((tweet) => {
        const tweetDate = new Date(tweet.created_at);
        return moment(tweetDate).isAfter(startDate) && moment(tweetDate).isAfter(sinceDate);
    });

    // Map the new found tweets for the BigQuery insert schema
    const bqRows = bigQueryMapper(tweetsFiltered);

    // Insert the new tweets into the BigQuery table
    const insertTweets = await insertTweetsToBigQuery(config, bqRows, eventPayload);
    console.log(insertTweets);
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
const insertTweetsToBigQuery = async (config, bqRows, eventPayload) => {
    // TODO: inputs should be table and rows, eventPayload is not needed here

    // BigQuery configurations
    const dataset = bigquery.dataset(config.bigQuery.datasetId);
    const table = dataset.table(config.bigQuery.insertTable);

    // Store the rows into BigQuery
    if (bqRows.length > 0) {
        console.log(`Inserting ${bqRows.length} rows.`);
        console.log(`Example row:\n${JSON.stringify(bqRows[0])}`)

        const bqPageSize = 500; // rows per one page in the streaming insert
        const bqPages = arrayPages(bqRows, bqPageSize);
        console.log(`Number of pages in the BQ streaming insert: ${bqPages}`);

        const bqPromises = [];
        for (let i = 0; i < bqPages; i++) {
            console.log('Paginating the insert request. Page number ' + i);
            var page = paginate(bqRows, bqPageSize, i);
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

const queryRequest = async (query, maxResults) => {
    // latestTweetsTable is a view in bigQuery that returns the id of the latest already collected tweet for each of the user names
    // insert options, raw: true means that the same rows format is used as in the API documentation
    const options = {
        maxResults: maxResults,
    };

    const results = await bigquery.query(query, options);

    return results[0];
}

async function fetchMultipleUserTweets(config, members) {
    // Set up the Twitter client
    const twitterClient = new Twitter({
        consumer_key: config.twitter.consumerKey,
        consumer_secret: config.twitter.consumerSecret,
        access_token_key: config.twitter.accessTokenKey,
        access_token_secret: config.twitter.accessTokenSecret
    });

    const tweetPromises = [];
    members.forEach(member => {
        const sinceId = member.max_id === null ? '1' : member.max_id;

        // Fetch all the tweets, for each of the users, that have not yet been collected
        try {
            const tweetPromise = twitterClient.get('statuses/user_timeline', {
                screen_name: member.user_screen_name,
                count: 200,
                include_rts: true,
                since_id: sinceId,
                tweet_mode: 'extended'
            }).catch((err) => {
                // catch some errors in the individual promises
                // e.g. "Sorry, that page does not xist. Code 34"
                console.log(`Error with user ${member.user_screen_name}: \n${JSON.stringify(err)}\n${err}`)
                return err;
            });

            if (tweetPromise) {
                tweetPromises.push(tweetPromise);
            }
        } catch (e) {
            console.log('Failed to make the user_timeline request.')
            console.log(e);
        }
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