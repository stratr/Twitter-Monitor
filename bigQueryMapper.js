function isRetweet(text) {
    return /^RT @/.test(text);
}

function checkUndefined(value) {
    if (typeof value === 'undefined' || value === undefined) {
        return null;
    } else {
        return value;
    }

}

exports.isRetweet = isRetweet;
function entityUrlMap(entities) {
    return entities.map(entity => {
        return { expanded_url: entity.expanded_url };
    });
}
exports.entityUrlMap = entityUrlMap;
function entityScreenNameMap(entities) {
    return entities.map(entity => {
        return { screen_name: entity.screen_name };
    });
}
exports.entityScreenNameMap = entityScreenNameMap;
function entityHashtagMap(entities) {
    return entities.map(entity => {
        return { text: entity.text };
    });
}
exports.entityHashtagMap = entityHashtagMap;
function entitySymbolsgMap(entities) {
    return entities.map(entity => {
        return { text: entity.text };
    });
}
exports.entitySymbolsgMap = entitySymbolsgMap;

function bigQueryMapper(tweets) {
    // Do some mapping to change the format to be more suitable for bigQuery
    // user metrics are "unnested" for easier access in BQ
    var rows = tweets.map(tweet => {
        return {
            "insertId": tweet.id_str,
            "json": {
                date: new Date(tweet.created_at).toISOString().slice(0, 10),
                date_string: tweet.created_at,
                id_str: tweet.id_str,
                text: tweet.text ? tweet.text : null,
                full_text: tweet.full_text ? tweet.full_text : null,
                truncated: tweet.truncated,
                source: tweet.source,
                entities_user_mentions: entityScreenNameMap(tweet.entities.user_mentions),
                entities_hashtags: entityHashtagMap(tweet.entities.hashtags),
                entities_symbols: entitySymbolsgMap(tweet.entities.symbols),
                entities_urls: entityUrlMap(tweet.entities.urls),
                in_reply_to_status_id_str: tweet.in_reply_to_status_id_str,
                in_reply_to_user_id_str: tweet.in_reply_to_user_id_str,
                in_reply_to_screen_name: tweet.in_reply_to_screen_name,
                user_id: tweet.user.id_str,
                user_name: tweet.user.name,
                user_screen_name: tweet.user.screen_name,
                user_location: tweet.user.location,
                user_description: tweet.user.description,
                user_url: tweet.user.url,
                user_followers: tweet.user.followers_count,
                user_friends: tweet.user.friends_count,
                user_created_at: tweet.user.created_at,
                user_listed_count: tweet.user.listed_count,
                user_favourites: tweet.user.favourites_count,
                user_statuses: tweet.user.statuses_count,
                user_image: tweet.user.profile_image_url_https,
                is_quote_status: tweet.is_quote_status,
                retweet_count: tweet.retweet_count,
                favorite_count: tweet.favorite_count,
                favorited: tweet.favorited,
                retweeted: tweet.retweeted,
                possibly_sensitive: checkUndefined(tweet.possibly_sensitive),
                lang: tweet.lang,
                is_retweet: isRetweet(tweet.text),
                full_json_string: JSON.stringify(tweet)
            }
        };
    });
    return rows;
}
exports.bigQueryMapper = bigQueryMapper;