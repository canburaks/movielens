import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array

class MovieManager():
    def __init__(self, movie_csv_path):
        self.df = pd.read_csv(movie_csv_path)

    def get_by_id(self, movie_id):
        return self.df[self.df["movie_id"] == movie_id]

    def filter_by_movie_id(self, movie_id_list):
        return self.df[self.df["movie_id"].isin(movie_id_list)]


class TagInfoManager():
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)

    def tag_id_set(self):
        return self.df["tag_id"].tolist()

    def tag_name_set(self):
        return self.df["tag_name"].tolist()


class TagScoreManager():
    def __init__(self, tag_score_csv_path):
        self.df = pd.read_csv(tag_score_csv_path)

    def filter_by_movie_id(self, movie_id_list):
        return self.df[self.df["movie_id"].isin(movie_id_list)]

    def filter_by_tag_id(self, tag_id_list):
        return self.df[self.df["tag_id"].isin(tag_id_list)]

    def filter_by_movie_and_tag(self, movie_id_list, tag_id_list):
        return self.df[self.df["movie_id"].isin(movie_id_list) & self.df["tag_id"].isin(tag_id_list)]


ag_score_file = "/home/jb/Projects/Github/movielens/data/vtagscore.csv"


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
        pivot_table = pd.pivot_table(self.df, index=["user_id"], columns=[
                                     "movie_id"], values=["rating"])
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

    def create_user_movie_matrix(self, sample_size, save_path, fillna=True, values=False, type="pickle"):
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

    def get_user_id_list(self, min, max=2500):
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
