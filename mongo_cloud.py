# CISC 5950: Big Data Programming
# Cassandra Big Data Project - MongoDB Test
#
# One of the key portions of this project was a speed test between Cassandra and another database used for Big Data - namely, MongoDB. This contains code for MongoDB.
#
# In this experiment, different numbers of test data (a pretend movie similar to that found in the movie database) are created and inserted into a mongoDB database. Each of these tests are timed, leading to statistics.
#
# Dakota Hernandez
# Last Updated: December 20, 2017

import pymongo
from pymongo import MongoClient
import datetime
import time

def main():
    client = MongoClient('104.154.187.162', username='root', password = 'o816l76cC8sl')

    db = client.moviedb
    posts = db.posts

    single_time = generate_test(1, posts)
    create_100_time = generate_test(100, posts)
    create_1000_time = generate_test(1000, posts)

    statistics(single_time, create_100_time, create_1000_time)


def generate_test(num_movies, posts):
    '''
    A series of num_movies pretend movies are inserted into the MongoDB database. This process is timed and the times are returned.
    The pretend movie we insert is themed after the Big Data ckass this project was created for. Although fictitious, it has the same fields as the actual movie database.
    '''
    create_times = 0
    new_posts = []
    start_time = time.time()
    for i in range(num_movies):
	new_posts.append({"adult": "FALSE",
	    "belongs_to_collection": " ",
	    "budget": "59500000",
	    "genres": "[{'id': 18, 'name': 'Drama'}, {'id': 53, 'name': 'Thriller'}]",
	    "homepage": "https://lpatruno.github.io/bigdata-fall17/",
	    "id": "100",
	    "imbd_id": "tt5950000",
	    "original_language": "en",
	    "original_title": "Cassandra VS Mongo",
	    "overview": "In the Speed Layer of the Lambda Architecture, fast speeds are of the utmost importance. Cassandra, a popular database, prides itself for being one of the fastest around. But when a new rival shows up, what is truly the best database?",
	    "popularity": "10.12345",
	    "poster_path": "/filler",
	    "production_companies": "Patruno Films",
	    "production_countries": "[{'iso_3166_1': 'US', 'name': 'United States of America'}]",
	    "release_date": "2017",
	    "revenue": "180000000",
	    "runtime": "135",
	    "spoken_lanuguages": "[{'iso_639_1': 'en', 'name': 'English'}]",
	    "status": "Released",
	    "tagline": "In the Speed Layer, only one can come out on top.",
	    "title": "Cassandra VS Mongo",
	    "video": "FALSE",
	    "vote_average": "8.4",
	    "vote_count": "5950"})
	result = posts.insert(new_posts[i])
    create_times = time.time() - start_time
    return create_times


def statistics(single_time, create_100_times, create_1000_times):
    print("Statistics:")
    print("Time for Single Insert: " + str(single_time))
    print("Time for 100 Inserts: " + str(create_100_times))
    print("Time for 1000 Inserts: " + str(create_1000_times))


if __name__ == '__main__':
    main()
