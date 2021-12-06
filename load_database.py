
import mysql.connector
from dotenv import load_dotenv
import os
import csv


good_terms = [
    'pretty good',
    'epic',
    'masterpiece',
    'amazing',
    '10/10',
    'favorite',
    'good',
    'great',
    'beautiful',
    'fun',
    'love',
    'best',
    'like'
]


bad_terms = [
    'bad',
    'terrible',
    'worst',
    'disappointing',
    'hate',
    'dislike',
    'not worth it',
    'don\'t buy',
    'dont buy',
    'trash',
    'regret'
]


def init_mysql_access():
    load_dotenv()
    username = os.getenv('MYSQL_USERNAME')
    password = os.getenv('MYSQL_PASSWORD')

    connection = mysql.connector.connect(username=username, password=password, host='localhost')
    cursor = connection.cursor()

    return connection, cursor


def compile_create_table_query(table_name, fields):
    fields = ','.join([
        f'{key} {fields[key]}'
        for key in fields.keys()
    ])
    return f'CREATE TABLE {table_name} ({fields})'


def create_tables(cursor):
    players = {
        'player_id': 'BIGINT PRIMARY KEY',
        'games_owned': 'INT',
        'num_reviews': 'INT'
    }

    games = {
        'game_id': 'BIGINT PRIMARY KEY',
        'name': 'VARCHAR(50)'
    }

    reviews = {
        'review_id': 'BIGINT PRIMARY KEY',
        'game_id': 'BIGINT',
        'player_id': 'BIGINT',

        'is_steam_purchase': 'BOOL',
        'received_for_free': 'BOOL',
        'written_during_early_access': 'BOOL',

        'time_created': 'DATETIME',
        'time_updated': 'DATETIME',
        'language': 'VARCHAR(30)',
        'review_text': 'TEXT',
        'is_positive': 'BOOL',
        'num_votes_up': 'INT',
        'num_votes_funny': 'INT',
        'weighted_vote_score': 'FLOAT',
        'num_comments': 'INT',

        'num_positive_words': 'INT',
        'num_negative_words': 'INT'
    }

    player_game_stats = {
        'player_id': 'BIGINT',
        'game_id': 'BIGINT',

        'playtime_forever': 'INT',          # minutes
        'playtime_last_two_weeks': 'INT',   # minutes
        'playtime_at_review': 'INT',        # minutes
        'time_last_played': 'DATETIME'
    }

    game_review_stats = {
        'game_id': 'BIGINT PRIMARY KEY',
        'num_positive_reviews': 'INT',
        'num_negative_reviews': 'INT',
        'pct_positive': 'FLOAT',
        'num_positive_funny': 'INT',
        'num_negative_funny': 'INT',
        'pct_funny_positive': 'FLOAT',
        'mean_weighted': 'FLOAT'
    }

    cursor.execute(compile_create_table_query('players', players))
    cursor.execute(compile_create_table_query('games', games))
    cursor.execute(compile_create_table_query('reviews', reviews))
    cursor.execute(compile_create_table_query('player_game_stats', player_game_stats))
    cursor.execute(compile_create_table_query('game_review_stats', game_review_stats))


def count_occurences(s, terms):
    count = 0
    for term in terms:
        if s.find(term) != -1:
            count += 1
    return count


def load_row_into_db(cursor, row):
    if row is None:
        return

    # fixing problem where there is a comma in the review text
    if len(row) != 22:
        row = list(row)
        diff = len(row) - 22
        row[11] = ','.join(row[11:11+diff+1])
        row[12:12+diff] = row[12+diff:]
        for i in range(10):
            row.pop()
        row = tuple(row)
    assert len(row) == 22

    (game_id, game_name, player_id, num_games_owned, num_reviews, playtime_forever, playtime_last_two_weeks,
     playtime_at_review, time_last_played, review_id, language, review_text, time_created, time_updated,
     is_positive, num_votes_up, num_votes_funny, weighted_vote_score, num_comments, is_steam_purchase,
     received_for_free, written_during_early_access) = row

    num_positive_words = count_occurences(review_text, good_terms)
    num_negative_words = count_occurences(review_text, bad_terms)

    cursor.execute(f'INSERT IGNORE INTO players VALUES ({player_id}, {num_games_owned}, {num_reviews});')
    cursor.execute(f'INSERT IGNORE INTO games VALUES ({game_id}, \'{game_name}\');')
    cursor.execute(f'INSERT INTO reviews VALUES ('
                   f'{review_id}, {game_id}, {player_id}, {is_steam_purchase}, {received_for_free},'
                   f'{written_during_early_access}, FROM_UNIXTIME({time_created}), FROM_UNIXTIME({time_updated}),'
                   f'\'{language}\', {review_text}, {is_positive}, {num_votes_up}, {num_votes_funny},'
                   f'{weighted_vote_score}, {num_comments}, {num_positive_words}, {num_negative_words});')
    cursor.execute(f'INSERT INTO player_game_stats VALUES ('
                   f'{player_id}, {game_id}, {playtime_forever}, {playtime_last_two_weeks},'
                   f'{playtime_at_review}, FROM_UNIXTIME({time_last_played}));')


def main():
    DATABASE_NAME = 'steam'

    connection, cursor = init_mysql_access()

    cursor.execute(f'DROP DATABASE IF EXISTS {DATABASE_NAME};')
    cursor.execute(f'CREATE DATABASE {DATABASE_NAME};')
    cursor.execute(f'USE {DATABASE_NAME};')

    create_tables(cursor)

    with open('reviews.csv') as in_file:
        data = csv.reader(in_file)
        for i, row in enumerate(data):
            load_row_into_db(cursor, row)

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
