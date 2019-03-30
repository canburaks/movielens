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


