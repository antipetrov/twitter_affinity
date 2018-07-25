import unittest
from twitter_parse import parse_tweet_line, TweetData, TweetParseError


class ParseCase(unittest.TestCase):

    def test_parse_line_valid(self):

        with open('test_line.txt', 'r') as f:
            line = f.readline()

        result = parse_tweet_line(line)

        self.assertIsInstance(result, TweetData)
        self.assertEqual(result.location, "solene  #c")
        self.assertEqual(result.name, "osnapitzlvn")
        self.assertEqual(result.tweet_text,
                         "RT @oihanamarre: PTDDDDDDDDR LE PAUVRE http://t.co/HUvnDpgggb")
        self.assertEqual(result.lang, "fr")
        self.assertEqual(result.country_code, "")
        self.assertEqual(result.created_at, "Sun Aug 16 21:41:13 +0000 2015")
        self.assertEqual(result.display_url,
                         "https://twitter.com/osnapitzlvn/status/633030779610775552")

    def test_parse_line_invalid(self):

        with open('test_line_invalid.txt', 'r') as f:
            line = f.readline()

        with self.assertRaises(TweetParseError):
            parse_tweet_line(line)


if __name__ == '__main__':
    unittest.main()
