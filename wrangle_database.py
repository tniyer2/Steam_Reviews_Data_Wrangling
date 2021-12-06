
import mysql.connector
from dotenv import load_dotenv
import os
import csv


def init_mysql_access():
    load_dotenv()
    username = os.getenv('MYSQL_USERNAME')
    password = os.getenv('MYSQL_PASSWORD')

    connection = mysql.connector.connect(username=username, password=password, host='localhost')
    cursor = connection.cursor()

    return connection, cursor


def main():
    DATABASE_NAME = 'steam'

    connection, cursor = init_mysql_access()

    cursor.execute(f'USE {DATABASE_NAME};')

    # issue queries
    cursor.execute(
        f'INSERT INTO game_review_stats (game_id) SELECT game_id FROM games;')
    cursor.execute(
        f'UPDATE game_review_stats grs SET num_positive_reviews=(SELECT COUNT(*) FROM reviews WHERE is_positive=TRUE AND game_id=grs.game_id);')
    cursor.execute(
        f'UPDATE game_review_stats grs SET num_negative_reviews=(SELECT COUNT(*) FROM reviews WHERE is_positive=FALSE AND game_id=grs.game_id);')
    cursor.execute(
        f'UPDATE game_review_stats grs SET num_positive_funny=(SELECT SUM(num_votes_funny) FROM reviews WHERE is_positive=TRUE AND game_id=grs.game_id);')
    cursor.execute(
        f'UPDATE game_review_stats grs SET num_negative_funny=(SELECT SUM(num_votes_funny) FROM reviews WHERE is_positive=False AND game_id=grs.game_id);')
    cursor.execute(
        f'UPDATE game_review_stats SET pct_funny_positive=num_positive_funny/(num_positive_funny + num_negative_funny) WHERE num_negative_funny > 0;')
    cursor.execute(
        f'UPDATE game_review_stats SET pct_positive=num_positive_reviews/(num_positive_reviews + num_negative_reviews) WHERE num_negative_reviews > 0;')


    connection.commit()
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
