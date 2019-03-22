import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array

cwd = os.getcwd()
data_folder = cwd + "/data/"

movie_file = data_folder + "movies.csv"
rating_file = data_folder + "ratings.csv"
tags_file = data_folder + "tags.csv"
gscore_file = data_folder + "genome-scores.csv"
gtags_file = data_folder + "genome-tags.csv"
link_file = data_folder + "links.csv"

#Validated Data files
valid_movie_file = data_folder + "vmovies.csv"
valid_rating_file = data_folder + "vratings.csv"
valid_tag_file = data_folder + "vtags.csv"
valid_tag_score_file = data_folder + "vtagscore.csv"


class DataValidation():
    def regex_year(value):
        import re
        if value:
            mo =re.search(r'.\d\d\d\d.', value)
            if mo:
                result = re.search(r'\d\d\d\d', value)
                return result.group()
            else:
                return 0

    def imdb_id_converter(value):
        length_of_value = len(str(value))
        missing_digit = 7 - length_of_value
        new_digits = str(0)*missing_digit + str(value)
        imdb_id_str = "tt{}".format(new_digits)
        return imdb_id_str

    def tmdb_id_int_form(value):
        try:
            return int(float(value))
        except:
            return 0

    def merge_movie_link():
        mdf = pd.read_csv(movie_file)
        ldf = pd.read_csv(link_file)
        return pd.merge(mdf, ldf, on="movieId", how="outer")

    def validate_movies():
        ndf = DataValidation.merge_movie_link()
        ndf["name"] = ndf["title"].str.split("(").str[0].str.strip()
        ndf["year"] = ndf["title"].apply( lambda x:  DataValidation.regex_year(x)).astype(np.int64)
        ndf["movie_id"] = ndf["movieId"].astype(np.int64)
        ndf["imdb_id"] = ndf["imdbId"].apply(lambda x: DataValidation.imdb_id_converter(x))
        ndf["tmdb_id"] = ndf["tmdbId"].apply(lambda x: DataValidation.tmdb_id_int_form(x))
        new_df = ndf[["movie_id","imdb_id", "tmdb_id", "name", "year"]]
        return new_df

    def drop_timestamp():
        df = pd.read_csv(rating_file)
        df["user_id"] = df["userId"]
        df["movie_id"] = df["movieId"]
        ndf = df[[ "user_id", "movie_id", "rating" ]]
        return ndf

    def drop_lessers(minimum=15):
        df = DataValidation.drop_timestamp()
        user_list = df["user_id"].unique().tolist()
        valid_users = []

        for user in tqdm(user_list):
            if df[df["user_id"]==user].shape[0] >= minimum:
                valid_users.append(user)
        return df[df["user_id"].isin(valid_users)]


    def create_valid_movies():
        newdf = DataValidation.validate_movies()
        newdf.to_csv(valid_movie_file, index=False)

    def create_valid_ratings(drop_lessers=True):
        if drop_lessers:
            newdf = DataValidation.drop_lessers()
            newdf.to_csv(valid_rating_file, index=False)
        else:
            newdf = DataValidation.drop_timestamp()
            newdf.to_csv(valid_rating_file, index=False)

    def create_valid_tag_score():
        df = pd.read_csv(gscore_file)
        df["movie_id"] = df["movieId"]
        df["tag_id"] = df["tagId"]
        ndf = df[["movie_id", "tag_id", "relevance"]]
        ndf.to_csv(valid_tag_score_file,  index=False)

    def create_valid_tags():
        df = pd.read_csv(gtags_file)
        df["tag_id"] = df["tagId"]
        df["tag_name"] = df["tag"]
        ndf = df[["tag_id", "tag_name"]]
        ndf.to_csv(valid_tag_file,  index=False)

class Movie():
    df = pd.read_csv(valid_movie_file)

    def __init__(self, movie_id):
        self.id = movie_id
        self.name = Movie.df[Movie.df["movie_id"]==self.id].iloc[0,3]

    def get(self):
        return df[df["movie_id"]==self.id]



    def get_tags(self, minimum=0.5):
        tag_score = pd.read_csv(valid_tag_score_file)
        tag_dict = pd.read_csv(valid_tag_file)

        self_score_df = tag_score[ (tag_score["movie_id"]==self.id) &  (tag_score["relevance"]>=minimum)].sort_values(by="relevance", ascending=False)
        self_score_df["tag_name"] = self_score_df["tag_id"].apply(lambda x: Tag.get_name(x))
        return self_score_df

    def get_tag_score(self, minimum=0.5):
        tag_score = pd.read_csv(valid_tag_score_file)
        score_df = tag_score[(tag_score["movie_id"]==self.id) & (tag_score["relevance"]>0.5)]
        return score_df


class Tag():
    tag_dict = pd.read_csv(valid_tag_file)
    tag_score_df = pd.read_csv(valid_tag_file)

    def get_name(tag_id):
        return Tag.tag_dict[Tag.tag_dict["tag_id"]==tag_id].iloc[0,1]



############################################################################
f5000 = "/home/jb/Documents/Data/UM-Matrix/f5000.pickle"
# CREATE BIG USER MOVIE MATRIX
d = pickson.get_msgpack("/home/jb/Documents/Data/dummy_list_dict.msgpack")

indices = []
list_dict = []
movie_id_set = set()

for i in tqdm(d):
    ratings = i.get("ratings")
    index = i.get("user_id")

    indices.append(index)
    list_dict.append(ratings)
    movie_id_set.update(list(ratings.keys()))






big_m = pd.DataFrame(columns=movie_id_set)
#for i in tqdm(range(100)):
for i in tqdm(range(len(indices))):
    udf = pd.DataFrame(list_dict[i], index=[indices[i]])
    big_m = big_m.append(udf)
big_m = big_m.fillna(0)
big_m = big_m.values
pickson.save_pickle("big_m,/home/jb/Documents/Data/user_movie_matrice_f5000.pickle" )
