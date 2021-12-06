
import os
import dotenv
import requests
import json
import time


def process_reviews_result(result):
    success = result['success'] == 1
    reviews_list = result['reviews']
    cursor = result['cursor']
    return success, reviews_list, cursor


def process_author(author):
    line = str(author["steamid"]) + ', '\
        + str(author["num_games_owned"]) + ', '\
        + str(author["num_reviews"]) + ', '\
        + str(author["playtime_forever"]) + ', '\
        + str(author["playtime_last_two_weeks"]) + ', '\
        + str(author["playtime_at_review"]) + ', '\
        + str(author["last_played"])
    return line


def process_review(review):
    line = str(review["recommendationid"]) + ', ' +\
        review["language"] + ', ' +\
        json.dumps(review["review"]) + ', ' +\
        str(review["timestamp_created"]) + ', ' +\
        str(review["timestamp_updated"]) + ', ' +\
        str(review["voted_up"]) + ', ' +\
        str(review["votes_up"]) + ', ' +\
        str(review["votes_funny"]) + ', ' +\
        str(review["weighted_vote_score"]) + ', ' +\
        str(review["comment_count"]) + ', ' +\
        str(review["steam_purchase"]) + ', ' +\
        str(review["received_for_free"]) + ', ' +\
        str(review["written_during_early_access"])
    return line


def append_reviews(file, reviews, game_id, game_name):
    for review in reviews:
        author = review['author']
        line = game_id + ', ' + game_name + ', ' + process_author(author) + ', ' + process_review(review)
        print(line, file=file)


def main():
    dotenv.load_dotenv()

    steam_key = os.getenv('STEAM_KEY')
    game_id = os.getenv('GAME_ID')
    game_name = os.getenv('GAME_NAME')

    base_url = 'https://store.steampowered.com/'
    end_point = 'appreviews/' + game_id + '/'
    url = base_url + end_point

    params = {
        'json': 1,
        'cursor': '*',
        'key': steam_key,
        'num_per_page': '100',
        'filter': 'recent',
        'language': 'english'
    }

    with open('a.csv', 'a') as out_file:
        while True:
            num_requests = 0
            while num_requests < 4000:
                num_requests += 1

                reviews_result = requests.get(url, params=params)
                reviews_result = reviews_result.json()
                success, reviews_list, cursor = process_reviews_result(reviews_result)

                if not success:
                    raise ValueError('Request Failed.')

                append_reviews(out_file, reviews_list, game_id, game_name)

                if cursor is None or not reviews_list:
                    print('Finished Downloading.')
                    return
                else:
                    params['cursor'] = cursor
            print('sleeping')
            time.sleep(3600)
            print('finished sleeping')


if __name__ == '__main__':
    main()
