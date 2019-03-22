import redis
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array
import _pickle as pickle
import random

pickle_folder = "./pickle"
index_file_path = "./pickle/index.msgpack"
list_dict_file_path = "./pickle/list_dict.msgpack"
#choose one movie id set, 20k or 15k
#movie_id_set_file_path = "./pickle/movie_id_filtered_set.pickle"
movie_id_set_file_path = "./pickle/movie_id_filtered_set15k.pickle"

big_matrix_path = "./pickle/big_matrix.pickle"


#REDIS PART
redis1 = redis.Redis(host='localhost', port=6379, db=1)
#redis1.get("1")
#b"{'307': 3.5, '481': 3.5, '1091': 1.5, '1257': 4.5, '1449': 4.5, '1590': 2.5, '1591': 1.5, '2134': 4.5, '2478': 4, '2840': 3, '2986': 2.5, '3020': 4, '3424': 4.5, '3698': 3.5, '3826': 2, '3893': 3.5}"


#BYTES TO DICT FUNCTION
def b2d(bytes_obj):
    import ast
    decoded = bytes_obj.decode("utf-8")
    result = ast.literal_eval(decoded)
    return result


indices = pickson.get_msgpack(index_file_path)
indices =  random.sample(indices, 30000)
movie_id_set = pickson.get_pickle(movie_id_set_file_path)

def create_matrix():
    big_matrix = []
    for uid in tqdm(indices):
        user_array = array("d", [])
        #user ratings dictionary
        user_ratings = b2d(redis1.get(str(uid)))
        if len(user_ratings.keys())<50:
            continue
        #iterate over the all movie ids
        for mid in movie_id_set:
            #user rating about the movie
            rating = user_ratings.get(str(mid))
            if rating:
                user_array.append(rating)
            elif rating==None:
                user_array.append(0)
        big_matrix.append(user_array)
    pickson.save_pickle(np.array(big_matrix), big_matrix_path)


create_matrix()

#GET USER MOVIE MATRIX
bm = pickson.get_pickle(big_matrix_path)
