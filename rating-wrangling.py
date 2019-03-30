import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array
os.chdir('/home/jb/Projects/Github/movielens')
from managers import MovieManager, TagInfoManager, TagScoreManager

pd.set_option('display.max_columns', 800)
pd.set_option('display.max_rows', 800)

#Validated Data files save path
valid_movie_file = "/home/jb/Projects/Github/movielens/data/vmovies.csv"
valid_rating_file = "/home/jb/Projects/Github/movielens/data/vratings.csv"
valid_tag_file = "/home/jb/Projects/Github/movielens/data/vtags.csv"
valid_tag_score_file = "/home/jb/Projects/Github/movielens/data/vtagscore.csv"


class RatingManager():
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)

    def filter_by_movie_id(self, movie_id_list):
        return self.df[self.df["movie_id"].isin(movie_id_list)]
    
    def filter_by_user_id(self, user_id_list):
        return self.df[self.df["user_id"].isin(user_id_list)]

    def transpose(df):
        return pd.pivot_table(df, index=["user_id"], columns=["movie_id"], values=["rating"])

    def transpose_with_mean(self):
        pivot_table = pd.pivot_table(self.df, index=["user_id"], columns=["movie_id"], values=["rating"])
        pivot_table.insert(0, "mean", pivot_table.mean(axis=1).round(4))
        pivot_table.reset_index(inplace=True)
        return pivot_table

    def create_user_mean_series(self, save_path):
        mean = self.df.groupby("user_id").agg(np.mean).iloc[:, 1].to_frame()
        print("mdf head", mean.head())
        mean["mean"] = mean["rating"]
        mean = mean.iloc[:, [1]]
        mean.reset_index(inplace=True)
        print("mdf2", mean.head())
        mean.to_csv(save_path, index=False)

    def create_user_quantity_series(self, save_path):
        df = self.df.groupby("user_id").agg(np.size).iloc[:, 1].to_frame()
        df["quantity"] = df["rating"]
        df = df.iloc[:, [1]]
        df.reset_index(inplace=True)
        df["quantity"] = df["quantity"].astype("int")
        df.to_csv(save_path, index=False)

    def create_user_movie_matrix(self,sample_size, save_path,fillna=True,values=False, type="pickle"):
        sampled = RM.get_sample(sample_size)
        um_matrix = RatingManager.transpose(sampled)
        if fillna:
            um_matrix = um_matrix.fillna(0)
        if values:
            um_matrix = um_matrix.values
        if type.startswith("p"):
            pickson.save_pickle(um_matrix, save_path)
        elif type.startswith("m"):
            pickson.save_msgpack(um_matrix, save_path)
        if type.startswith("c"):
            um_matrix.to_csv(save_path, index=False)

    def get_user_quantity_series(self):
        df = self.df.groupby("user_id").agg(np.size).iloc[:, 1].to_frame()
        df["quantity"] = df["rating"]
        df = df.iloc[:, [1]]
        df.reset_index(inplace=True)
        df["quantity"] = df["quantity"].astype("int")
        df = df.sort_values("quantity", ascending=False)
        return df

    def get_user_id_list(self,min, max=2500):
        qs = self.get_user_quantity_series()
        ndf = qs[(qs["quantity"] > min) & (qs["quantity"] < max)]
        return ndf["user_id"].unique()

    def get_sample(self, sample_size):
        import random
        user_id_list = self.df["user_id"].tolist()
        random_sample_id_list = random.sample(user_id_list, sample_size)
        return self.df[self.df["user_id"].isin(random_sample_id_list)]


    def save(df, file_name, csv=True):
        folder = "/home/jb/Projects/Github/movielens/filtered-data/ratings/"
        if csv:
            df.to_csv(folder + file_name, index=False)
        else:
            df.to_excel(folder + file_name, sheet_name="sheet1", index=False)


#RM = RatingManager(valid_rating_file)
#movie_filtered_file = "/home/jb/Projects/Github/movielens/filtered-data/ratings/movie-filtered.csv"
filtered_file = "/home/jb/Projects/Github/movielens/filtered-data/ratings/movie-user-filtered.csv"
RM = RatingManager(filtered_file)

#TAKE RANDOM SAMPLE
"""rndf = RM.get_sample(20000)
udf = RatingManager.transpose(rndf)
udf.head()
RatingManager.save(udf, "user-movie-matrix-20k.csv")
"""
um20kpath = "/home/jb/Projects/Github/movielens/filtered-data/ratings/user-movie-matrix-20k-1.csv."
um20k_ppath = "/home/jb/Projects/Github/movielens/filtered-data/ratings/user-movie-matrix-20k.pickle."

RatingManager.create_user_movie_matrix(RM, 20000, um20kpath, fillna=True, values=True, type="csv")

#Save user_id and mean df
#RM.create_user_mean_series("/home/jb/Projects/Github/movielens/filtered-data/ratings/user-mean.csv")

#Save user_id and quantity df
#RM.create_user_quantity_series("/home/jb/Projects/Github/movielens/filtered-data/ratings/user-quantity.csv")

#USER-QUANTITY SERIES
#uq = pd.read_csv("/home/jb/Projects/Github/movielens/filtered-data/ratings/user-quantity.csv")


#FILTER BY USER ID


#FILTER BY MOVIE
#filtered_movie_id_list = pickson.get_pickle("/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie_id_list.pickle")
#frm = RM.filter_by_movie_id(filtered_movie_id_list)
#RatingManager.save(frm, "movie-filtered.csv")

#FILTER BY USER ID
#user_filtered_file = "/home/jb/Projects/Github/movielens/filtered-data/ratings/movie-user-filtered.csv"
#user_id_list = RM.get_user_id_list(39, 2500)
#urm = RM.filter_by_user_id(user_id_list)
#RatingManager.save(urm, "movie-user-filtered.csv")
"""


mini = rdf.iloc[:20,:]
mean = mini.groupby("user_id").agg(np.mean).iloc[:,1].to_frame()
mean["mean"] = mean["rating"]
mean = mean.iloc[:,[1]]
mean.reset_index(inplace=True)
mean
pt =pd.pivot_table(mini, index=["user_id"], columns=["movie_id"], values=["rating"] )
pt.insert(0, "mean", pt.mean(axis=1).round(4))
pt.reset_index(inplace=True)
pt.index.levels[0]
pt.insert(0,"user_id", pt.index)

pt



rdf.movie_id.value_counts()[:10]
pt.groupby("user_id").agg(np.size)

pd.merge(mini, mean, on="user_id", how="inner")
mean

pt["mean"] = pt.rating.mean(axis=1)
pt
pt.insert(0, "average", pt["mean"])

mini
m1 = mini.groupby(["user_id", "movie_id", "rating"])[["rating"]].mean()
m1
pd.merge(mini, m1, on="user_id", how="inner" )


mini["mean"] = m1
mini
mini["user_id"].agg(np.size)
"""
