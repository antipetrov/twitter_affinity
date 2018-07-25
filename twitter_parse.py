import json
import sqlite3
import os
from optparse import OptionParser
from collections import namedtuple


TweetData = namedtuple("TweetData", ('name', 'tweet_text', 'country_code',
    'display_url', 'lang', 'created_at', 'location'))


class TweetParseError(Exception):
    pass


class TweetInsertError(Exception):
    pass


def parse_tweet_line(tweet_line):
    """
    decodes & parses line from log-file to retrieve
    (name, tweet_text, country_code, display_url, lang, created_at, location)
    @return dict
    """

    try:
        tweet_dict = json.loads(tweet_line)
    except json.decoder.JSONDecodeError as e:
        raise TweetParseError('Json-decode error {}'.format(e))

    # validate requred fields
    if not tweet_dict.get('user') or not tweet_dict.get('text') or \
            not tweet_dict.get('id'):

        raise TweetParseError('Invalid tweet record')

    # defaults
    place = tweet_dict.get('place', {}) or {}

    display_url = "https://twitter.com/{}/status/{}".format(
        tweet_dict['user']['screen_name'],
        tweet_dict['id'])

    result = TweetData(
        name=tweet_dict['user']['screen_name'],
        tweet_text=tweet_dict['text'],
        country_code=place.get('country_code', ''),
        display_url=display_url,
        lang=tweet_dict.get('lang', ''),
        created_at=tweet_dict['created_at'],
        location=tweet_dict['user'].get('location', ''),
    )

    return result


def save_tweet(tweet_data, cursor):
    """
    inserts tweetdata into sqlite database (singular tweets-table)
    """

    insert_sql = """
    INSERT INTO tweets (name, tweet_text, country_code,
    display_url, lang, created_at, location) VALUES (?,?,?,?,?,?,?)
    """

    try:
        cursor.execute(insert_sql, tuple(tweet_data))
    except sqlite3.Error as e:
        raise TweetInsertError('Database insert error {} in sql: {}'.
                               format(e, insert_sql))
    return True


def db_init(db_filename, cleanup=True):
    """
    connect to db, bootstrap tables if needed
    """

    try:
        connection = sqlite3.connect(db_filename)
        connection.isolation_level = None
        cursor = connection.cursor()
    except sqlite3.Error as e:
        print('Database connection error: {}'.format(e))
        return None

    # drop table (for cleanup)
    print('Cleanup: {}'.format(cleanup))
    if cleanup:
        print("dropping table `tweets`")
        drop_sql = 'DROP TABLE IF EXISTS "tweets";'
        cursor.execute(drop_sql)

    # create table
    create_sql = """
    CREATE TABLE IF NOT EXISTS "tweets"
    ("name" TEXT, "tweet_text" TEXT, "country_code" TEXT,
    "display_url" TEXT, "lang" TEXT, "created_at" TEXT, "location" TEXT,
    "tweet_sentiment" REAL);
    """
    cursor.execute(create_sql)

    return connection


def main():
    parser = OptionParser()
    parser.add_option("-c", "--cleanup", dest="cleanup", action="store_true",
                      help="Delete old data", default=False)
    parser.add_option("-f", "--tweetfile", dest="tweetfile", action="store",
                      help="tweets file",
                      default='./three_minutes_tweets.json.txt')
    parser.add_option("-d", "--dbfile", dest="dbfile", action="store",
                      help="Filename datafile", default="./tweets.db")
    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose", default=False,
                      help="print status messages to stdout")

    (options, args) = parser.parse_args()

    print('Start loading tweets')

    # locate tweetfile
    tweetfile_abspath = os.path.abspath(options.tweetfile)
    print('Source file: {}'.format(tweetfile_abspath))

    # locate dbfile
    datafile_abspath = os.path.abspath(options.dbfile)
    print('Database file: {}'.format(datafile_abspath))

    connection = db_init(datafile_abspath, options.cleanup)
    cursor = connection.cursor()

    line_count = 0
    processed_count = 0

    with open(tweetfile_abspath, 'r') as f:
        for line in f:
            line_count += 1

            if line_count % 200 == 0:
                print('{} lines processed, {} records saved'.format(
                    line_count, processed_count))

            try:
                tweet_data = parse_tweet_line(line)
            except TweetParseError as e:
                if options.verbose:
                    print('Line {} parse error: {}'.format(line_count, e))
                continue

            try:
                inserted = save_tweet(tweet_data, cursor)
            except TweetInsertError as e:
                inserted = False
                if options.verbose:
                    print('Line {} insert error: {}'.format(line_count, e))
                continue

            if inserted:
                processed_count += 1

    print("Total lines processed: {}\nTotal records inserted: {}".format(
        line_count, processed_count))

    cursor.close()


if __name__ == '__main__':
    main()
