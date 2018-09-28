# Загрузка твитов

Скрипт для загрузки твитов

`python twitter_parse.py`

Параметры:

"-f", "--tweetfile" - путь к json-файлу с твитами, по-умолчанию './three_minutes_tweets.json.txt'

"-d", "--dbfile" - путь к файлу БД sqlite, по-умолчанию "./tweets.db"

"-с" "--cleanup" - удалить данные из БД (drop table) перед загрузкой, по-умолчанию False

"-v", "--verbose" - расширенный вывод в stdout, по-умолчанию False

Данные записываются в таблицу tweets, если таблицы в БД нет, она создается.

Базовые тесты парсера - test_parser.py



# Подсчет tweet_sentiment

(предполагается что данные разбиты по двум таблицам: tweets_post, tweets_user)

Для подсчета средней оценки нужно выполнить 

`python calc_sentiment.py `

"-a", "--afinnfile" - путь к AFINN-файлу, по-умолчанию "./AFINN-111.txt"

"-d", "--datafile" - путь к файлу БД sqlite, по-умолчанию "./tweets.db"

"-v", "--verbose" - расширенный вывод в stdout, по-умолчанию False

# Вывод статистики по счастливым\несчастным странам-локациям-пользователям

Select-запрос - в файле `aggregates.sql`

Колонки: 

- `most_happy_user` - самый счастливый пользователь
- `least_happy_user` - самый несчастный пользователь
- `most_happy_country` - самая счастливая страна
- `least_happy_country` - самая несчастная страна
- `most_happy_location` - самый счастливый location
- `least_happy_location`  - самый несчастный location

