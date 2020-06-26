# Twitter monitor

A node.js app that runs on Google Cloud as a Cloud Function periodically with a Pub/Sub trigger. Collects tweets by the Twitter users that are listed on a defined twitter list.

Functionality:

1. Check BigQuery for the latest already collected tweets for each of the followed members. Relies on a view in BQ.
2. Get all the members from the defined Twitter list (the list must exist in Twitter). Combine the previously fetched information with these: what was the latest already collected tweet?
3. Get all new tweets by those users.
4. Map the tweet data to fit the defined BigQuery schema and handle request pagination.
5. Store all the fetched tweets in a BigQuery table.

API keys and some other configuration details are stored in Cloud Storage. Some configs could be moved to the PubSub event payload to make the function more reusable.