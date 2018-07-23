import json


def process_tweet(tweet_obj):
    return None


def main():
    line_num = 0
    for line in open('./three_minutes_tweets.json.txt', 'r'):
        line_num += 1
        try:
            tweet_obj = json.loads(line)
            process_tweet(tweet_obj)
        except json.decoder.JSONDecodeError as e:
            print('error {} in line {}'.format(type(e), line_num))


if __name__ == 'main':
    main()
