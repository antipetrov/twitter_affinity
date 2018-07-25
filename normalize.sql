CREATE TABLE IF NOT EXISTS "tweets_post"
    (id INTEGER PRIMARY KEY, "user_name" INTEGER, "tweet_text" TEXT, 
    "display_url" TEXT, "lang" TEXT, "created_at" TEXT, 
    "tweet_sentiment" REAL);

CREATE TABLE IF NOT EXISTS "tweets_user"
    ("name" TEXT UNIQUE, "country_code" TEXT, 
    "location" TEXT);

INSERT INTO tweets_user SELECT null, name, country_code, location, 0 FROM tweets GROUP BY tweets;
INSERT INTO tweets_post SELECT name, tweet_text, display_url, lang, created_at FROM tweets;
DROP TABLE tweets;

