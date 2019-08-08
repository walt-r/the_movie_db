import json
import requests
from time import sleep


MOVIE_DISC_QUERY = 'http://api.themoviedb.org/3/discover/movie?primary_release_date.gte={date1}&primary_release_date.lte={date2}&api_key={api_key}&page={page}'
TV_DISC_QUERY = 'http://api.themoviedb.org/3/discover/tv?first_air_date.gte={date1}&first_air_date.lte={date2}&api_key={api_key}&page={page}'

#English only credits gives different answer
MOVIE_CREDITS_QUERY = 'https://api.themoviedb.org/3/movie/{media_id}/credits?api_key={api_key}&language=en-US'
TV_CREDITS_QUERY = 'https://api.themoviedb.org/3/tv/{media_id}/credits?api_key={api_key}&language=en-US'

API_KEY = '606aaffd7ca10f0b80804a1f0674e4e1'
STATUS_UPDATE = 20  # print status about every 5 sec
WAIT_TIME = 2
MAX_RETRY = 5

ACTOR_ID_TYPE = 'id'  # 'name' gives different answer

def query_credits(media_id, base_query):
    """Queries database for credits of a movie or tv show identified by id.
    returns the full response in json format
    """
    query = base_query.format(api_key=API_KEY, media_id=media_id)
    retry = 0
    done = False
    while not done:
        try:
            response = requests.get(query)
            if response.status_code != 200:
                response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(e, f"retry after {WAIT_TIME} second wait")
            retry += 1
            if retry > MAX_RETRY:
                print(f"Quiting after {MAX_RETRY} retries")
                raise requests.exceptions.HTTPError
            sleep(WAIT_TIME)
        else:
            done = True    
    return response.json()

def query_discovery_page(query):
    """Queries database for list of a movie or tv shows.  Retrieves one page
    at a time.
    Returns total page number for that query the full response in json format
    """
    retry = 0
    done = False
    while not done:
        try:
            response = requests.get(query)
            if response.status_code != 200:
                response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(e, f"retry after {WAIT_TIME} second wait")
            retry += 1
            if retry > MAX_RETRY:
                print(f"Quiting after {MAX_RETRY} retries")
                raise requests.exceptions.HTTPError
            sleep(WAIT_TIME)
        else:
            done = True
    total_pages = response.json().get('total_pages')
    results = response.json().get('results')
    return total_pages, results

def get_media_ids(first_date, last_date, base_query):
    """Retrieves a list of all the media ids from database within a 
    date range
    """
    page = 1
    query = base_query.format(api_key=API_KEY, page=page, date1=first_date, date2=last_date)
    print(query)
    total_pages, medias = query_discovery_page(query)
    media_ids = []
    media_ids = media_ids + [media['id'] for media in medias]
    print(f'getting {total_pages} pages')
    while page <= total_pages:
        if page%STATUS_UPDATE == 0:  # give status every 5 seconds
            print('Retrieving page {}'.format(page))
        if (page > 1):
            query = base_query.format(api_key=API_KEY, page=page, date1=first_date, date2=last_date)
            response = requests.get(query)
            _, medias = query_discovery_page(query) # response.json()['results']
        new_ids = [media['id'] for media in medias]
        media_ids = media_ids + new_ids
        page += 1
    return media_ids

def get_cast_ids(media_id, query):
    """Retrieves the list of actor ids for a given movie or tv show
    by id.
    """
    credits = query_credits(media_id, query)
    actor_ids = []
    for actor in credits['cast']:
        actor_ids.append(actor[ACTOR_ID_TYPE])
    return actor_ids

def get_actor_ids(media_ids, base_query):
    """Retrieves a list of actor ids for an entire list of media ids.
    """
    all_movie_actor_ids = []
    for i, movie_id in enumerate(media_ids):
        movie_actor_ids = get_cast_ids(movie_id, base_query)
        all_movie_actor_ids.extend(movie_actor_ids)
        if i%STATUS_UPDATE == 0:  # give status every 5 seconds
            print('Current total of actor ids is {}'.format(len(all_movie_actor_ids)))
    return all_movie_actor_ids

def two_media_actors(tv_actor_ids, movie_actor_ids):
    """Returns intersection of two lists
    """
    movie_set = set(movie_actor_ids)
    tv_set = set(tv_actor_ids)
    return tv_set.intersection(movie_set)


if __name__ == '__main__': 
    # give dates as input args to cmdline or job
    first_date = '2018-12-01'
    last_date = '2018-12-31'

    movie_ids = get_media_ids(first_date, last_date, base_query=MOVIE_DISC_QUERY)
    print('Retrieved a total of {} movie ids'.format(len(movie_ids)))

    tv_ids = get_media_ids(first_date, last_date, base_query=TV_DISC_QUERY)
    print('Retrieved a total of {} tv ids'.format(len(tv_ids)))

    print('Retrieving movie actors')
    movie_actor_ids = get_actor_ids(movie_ids, MOVIE_CREDITS_QUERY)

    print('Retrieving tv actors')
    tv_actor_ids = get_actor_ids(tv_ids, TV_CREDITS_QUERY)

    busy_actors = two_media_actors(tv_actor_ids, movie_actor_ids)
    print('\nBetween the dates {} and {}, {} actors appear in both movie and tv.'.\
          format(first_date, last_date, len(busy_actors)))
