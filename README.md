# Twitter monitor

A node.js app that runs on Google Cloud as a Cloud Function periodically with a Pub/Sub trigger. Collects tweets by the Twitter users that are listed on a defined twitter list.

Functionality:

1. Check BigQuery for the latest already collected tweets for each of the followed members.
2. Get all the members from the defined Twitter list. Combine the previously fetched information with these.
3. Get all new tweets by those users.
4. Map the tweet data to fit the defined BigQuery schema and handle request pagination.
5. Store all the fetched tweets in BigQuery.

API keys and some other configuration details are stored in Cloud Storage.