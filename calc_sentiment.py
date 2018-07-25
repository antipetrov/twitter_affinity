import os
import string
import sqlite3
from collections import namedtuple
from optparse import OptionParser

TweetRec = namedtuple("TweetRec", ('id', 'text', 'user_name', 'sentiment'))


class TweetLoadError(Exception):
    pass


class TweetUpdateError(Exception):
    pass


def tweets_iter(cursor, chunk_size=500):
    tweets_sql = "SELECT id, tweet_text FROM tweets_post"

    cursor.execute(tweets_sql)
    results = cursor.fetchall()
    for result in results:
        yield result


def update_post_sentiment(cursor, post_id, sentiment_value):
    update_sql = "UPDATE tweets_post SET tweet_sentiment=? WHERE id=?"
    try:
        cursor.execute(update_sql, (sentiment_value, post_id))
    except sqlite3.Error as e:
        raise TweetUpdateError('Could not update row {} value {}. Error: {}'
            .format(post_id, sentiment_value, e))

    return True


def get_text_sentiment(text, weight_dict):
    """
    Подсчитывает sentiment как средний вес всех слов текста по словарю весов
    @return float
    """

    # split to words
    words = [word.strip(string.punctuation) for word in text.split()]

    if not words:
        return 0

    # calculate
    weight_sum = 0
    for word in words:
        weight_sum += weight_dict.get(word, 0)

    return float(weight_sum) / len(words)


def main():
    parser = OptionParser()
    parser.add_option("-a", "--afinnfile", dest="afinnfile",
                      action="store", help="affinity file",
                      default="./AFINN-111.txt")

    parser.add_option("-d", "--datafile", dest="dbfile",
                      action="store", help="database file",
                      default="./tweets.db")

    parser.add_option("-v", "--verbose",
                      action="store_true", dest="verbose",
                      help="don't print status messages to stdout")

    (options, args) = parser.parse_args()


    # locate afinn file   
    afinn_abspath = os.path.abspath(options.afinnfile)
    print('AFINN file: {}'.format(afinn_abspath))
    sentiment_dict = dict()
    try:
        with open(afinn_abspath) as f:
            for line_num, line in enumerate(f, 1):
                parts = line.strip().split('\t')

                try:
                    val = int(parts[1])
                    sentiment_dict[parts[0]] = val
                except ValueError as e:
                    print('parse error in line {}: {}'.format(line_num, e))

    except OSError as e:
        print("AFINN file {}: {}".format(afinn_abspath, e))

    # locate db file
    datafile_abspath = os.path.abspath(options.dbfile)
    print('Database file: {}'.format(datafile_abspath))

    # connect to db
    try:
        connection = sqlite3.connect(datafile_abspath)
        connection.isolation_level = None
        cursor = connection.cursor()
    except sqlite3.Error as e:
        print('Database connection error: {}'.format(e))
        exit()

    # process records
    loaded_count = 0
    updated_count = 0
    for row_id, text in tweets_iter(cursor):
        loaded_count += 1
        post_sentiment = get_text_sentiment(text, sentiment_dict)

        try:
            updated = update_post_sentiment(cursor, row_id, post_sentiment)
        except TweetUpdateError as e:
            if options.verbose:
                print("Update error: {}".format(e))
            continue

        updated_count += 1

        if loaded_count % 200 == 0:
            print('{} tweets processed, {} updated'.format(loaded_count, updated_count))

    print("Tweets loaded: {}\nTweets updated: {}".format(loaded_count, updated_count))


if __name__ == '__main__':
    main()