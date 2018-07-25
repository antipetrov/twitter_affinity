select 
(
    select user_name from tweets_post group by user_name order by sum(tweet_sentiment) desc limit 1
) as most_happy_user, 
(
    select user_name from tweets_post group by user_name order by sum(tweet_sentiment) limit 1

) as least_happy_user,
( 
    select country_code from tweets_post join tweets_user on (tweets_user.name=tweets_post.user_name) 
    where country_code!=''
    group by country_code order by SUM(tweet_sentiment) desc limit 1
) as most_happy_country,
( 
    select country_code from tweets_post join tweets_user on (tweets_user.name=tweets_post.user_name) 
    where country_code!=''
    group by country_code order by SUM(tweet_sentiment) limit 1
) as least_happy_country,
( 
    select country_code || '/' ||location from tweets_post join tweets_user on (tweets_user.name=tweets_post.user_name) 
    where country_code!='' and location!=''
    group by country_code, location order by SUM(tweet_sentiment) desc limit 1
) as most_happy_location,
( 
    select country_code || '/' ||location from tweets_post join tweets_user on (tweets_user.name=tweets_post.user_name) 
    where country_code!='' and location!=''
    group by country_code, location order by SUM(tweet_sentiment) limit 1
) as least_happy_location;