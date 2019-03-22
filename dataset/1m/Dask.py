import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, progress

import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson

#PANDAS DATAFRAME
pdf = pd.read_table("dataset/1m/ratings.dat", sep="::",engine="python", header=None)
pdf = pdf.iloc[:,:3]


#DASK DATAFRAME
client = Client(n_workers=2, threads_per_worker=2, memory_limit='2GB')
ddf = dd.read_table("dataset/1m/ratings.dat", sep="::",engine="python", header=None)
ddf = ddf.iloc[:,:3]


#Unique Values
movie_id_set = set(ddf.iloc[:,1].compute())
user_id_set = set(ddf.iloc[:,0].compute())


def UM(df):
    movie_id_set = set(rdf.iloc[:,1])
    user_id_set = set(rdf.iloc[:,0])
    big_matrice = []
    for user in tqdm(user_id_set):
        user_array = array("d", [])
        for movie in movie_id_set:
            qs = df[(df[0]==user) & (df[1]==movie)]
            #if not exist make the rating zero
            if len(qs)==0:
                user_array.append(0)
            #if user rate the movie append it to user array
            elif len(qs)==1:
                rating = qs.iloc[0,2]
                user_array.append(rating)
        big_matrice.append(user_array)
    big_matrice = np.array(big_matrice)
    pickson.save_pickle(big_matrice, "./user_movie_matrice.pickle")
