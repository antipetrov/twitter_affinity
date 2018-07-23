import json
import sqlite3
import os
from optparse import OptionParser


def process_tweet(tweet_obj, cursor):

    try:
        name = tweet_obj['user']['screen_name']
        tweet_text = tweet_obj['text']
        country_code = tweet_obj.get('country_code', '')
        display_url = tweet_obj.get('display_url', '')
        lang = tweet_obj['user']['lang']
        created_at = tweet_obj['created_at']
        location = tweet_obj['user']['location']
    except KeyError as e:
        raise

    insert_sql = """
    INSERT INTO tweets (name, tweet_text, country_code,
    display_url, lang, created_at, location) VALUES (?,?,?,?,?,?,?)
    """

    try:
        cursor.execute(insert_sql,
                       (name, tweet_text, country_code, display_url, lang, created_at, location))
    except sqlite3.Error as e:
        raise

    return True


def main():
    parser = OptionParser()
    parser.add_option("--cleanup", dest="cleanup",
                      action="store_true", help="Delete old data", default=True)
    parser.add_option("-f", "--dbfile", dest="dbfile",
                      action="store", help="Filename datafile", default="./tweets.db")
    parser.add_option("-v", "--verbose",
                      action="store_false", dest="verbose",
                      help="don't print status messages to stdout")

    (options, args) = parser.parse_args()

    print('Start loading tweets')

    # check datafile
    datafile_abspath = os.path.abspath(options.dbfile)
    print('Database file: {}'.format(datafile_abspath))

    # connect to db
    try:
        connection = sqlite3.connect(datafile_abspath, )
        connection.isolation_level = None
        cursor = connection.cursor()
    except sqlite3.Error as e:
        print('Database connection error: {}'.format(e))
        exit()

    # drop table (for cleanup)
    print('Cleanup: {}'.format(options.cleanup))
    if options.cleanup:
        print("dropping table")
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

    line_count = 0
    processed_count = 0

    for line in open('./three_minutes_tweets.json.txt', 'r'):
        line_count += 1
        try:
            tweet_obj = json.loads(line)

        except json.decoder.JSONDecodeError as e:
            print('Line {}: json error {}'.format(line_count, type(e)))
            continue

        try:
            processed = process_tweet(tweet_obj, connection)
        except KeyError:
            processed = False
            if options.verbose:
                print('Line {}: Database insert error: {} in sql'.\
                    format(line_count, e))
            pass

        except sqlite3.Error:
            processed = False
            if options.verbose:
                print('Line {}: Database insert error: {} in sql'.\
                      format(line_count, e))
            pass

        if processed:
            processed_count += 1

        if line_count % 200 == 0:
            print('{} lines processed, {} records saved'.format(line_count, processed_count))

    print("Total lines processed: {}\nTotal records inserted: {}", (line_count, processed_count))


if __name__ == '__main__':
    main()
