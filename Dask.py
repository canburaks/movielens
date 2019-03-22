import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, progress

import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array

rating_file_path = "dataset/ml-latest/ratings.csv"


#PANDAS DATAFRAME
pdf = pd.read_csv(rating_file_path)
pdf = pdf.iloc[:,:3]

#DASK DATAFRAME
client = Client(n_workers=2, threads_per_worker=2, memory_limit='2GB')
ddf = dd.read_csv(rating_file_path)
ddf = ddf.iloc[:,:3]


#Unique Values
movie_id_set = set(ddf.iloc[:,1].compute())
user_id_set = set(ddf.iloc[:,0].compute())

ddf

#prepare dask dataframe
ddf = ddf.compute()
ddf.head()
def UM(df):
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
    pickson.save_pickle(big_matrice, "./pickle/user_movie_matrice.pickle")

UM(ddf)
