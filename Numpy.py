import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array

pickle_folder = "./pickle"
index_file_path = "./pickle/index.msgpack"
list_dict_file_path = "./pickle/list_dict.msgpack"
movie_id_set_file_path = "./pickle/movie_id_set.msgpack"

"""
msgpack_file = pickson.get_msgpack("/home/jb/Documents/Data/dummy_list_dict.msgpack")
#Segregations of index, ratings and movie_id_set
def segregation():
    # Ordered index and ratings. Plus movie id set
    indices = []
    list_dict = []
    movie_id_set = set()

    #Fill collections above from msgpack file
    for i in tqdm(msgpack_file):
        ratings = i.get("ratings")
        index = i.get("user_id")

        indices.append(index)
        list_dict.append(ratings)
        movie_id_set.update(list(ratings.keys()))
    pickson.save_msgpack(indices, index_file_path)
    pickson.save_msgpack(list_dict, list_dict_file_path)
    pickson.save_msgpack(list(movie_id_set), movie_id_set_file_path)
segregation()
"""

indices = pickson.get_msgpack(index_file_path)
ratings = pickson.get_msgpack(list_dict_file_path)
movie_id_set = pickson.get_msgpack(movie_id_set_file_path)

import redis
redis1 = redis.Redis(host='localhost', port=6379, db=1)

for i in tqdm(range(len(indices))):
    index = indices[i]
    rates = ratings[i]

    redis1.set(index, rates, None)
