import numpy as np
import pandas as pd
from tqdm import tqdm
from array import array
import pickson
rf = "./ratings.dat"
rdf = pd.read_table(rf, sep="::", header=None, engine="python")
rdf = rdf.iloc[:,:3]

#Make numpy matrice
matrice = rdf.values

#Unique Values
movie_id_set = set(rdf.iloc[:,1])
user_id_set = set(rdf.iloc[:,0])

data = pd.DataFrame()

#Create numpy Matrice with iteration
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


#Create User X MOVIE MATRIX
df = pd.DataFrame(columns=movie_id_set)
