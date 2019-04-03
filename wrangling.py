import pandas as pd
import numpy as np
import os
from tqdm import tqdm
import pickson
from array import array

os.chdir('/home/jb/Projects/Github/movielens')

pd.set_option('display.max_columns', 800)
pd.set_option('display.max_rows', 800)

movie_file = "/home/jb/Projects/Github/movielens/rawdata/movies.csv"
rating_file = "/home/jb/Projects/Github/movielens/rawdata/ratings.csv"
tags_file = "/home/jb/Projects/Github/movielens/rawdata/tags.csv"
gscore_file = "/home/jb/Projects/Github/movielens/rawdata/genome-scores.csv"
gtags_file = "/home/jb/Projects/Github/movielens/rawdata/genome-tags.csv"
link_file = "/home/jb/Projects/Github/movielens/rawdata/links.csv"

#Validated Data files save path
valid_movie_file = "/home/jb/Projects/Github/movielens/data/vmovies.csv"
valid_rating_file = "/home/jb/Projects/Github/movielens/data/vratings.csv"
valid_tag_file = "/home/jb/Projects/Github/movielens/data/vtags.csv"
valid_tag_score_file = "/home/jb/Projects/Github/movielens/data/vtagscore.csv"


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

    def create_valid_ratings(drop_lessers=False):
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

################################################################################################


from managers import MovieManager, TagInfoManager, TagScoreManager

Movies = MovieManager(valid_movie_file)
TagScore = TagScoreManager(valid_tag_score_file)

#---------------Change filtered path---------------------------------------------------> 
TagInfo = TagInfoManager("/home/jb/Projects/Github/movielens/filtered-data/mini_tags.csv")

#--------Lists------------------>
#tag id list
filtered_tag_id_list = TagInfo.tag_id_set()
#tag name list
filtered_tag_name_list = TagInfo.tag_name_set()
#movie id list
filtered_movie_id_list = pickson.get_pickle("/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie_id_list.pickle")

#--------Dataframes------------------>
#movie dataframe with id, name and year
filtered_movie_df = Movies.filter_by_movie_id(filtered_movie_id_list).iloc[:, [0,3,4]]
#tag score dataframe with selected tags and movies, and reduce to 0 if relevance is low than min
filtered_tag_scores = TagScore.filter_by_movie_and_tag(filtered_movie_id_list, filtered_tag_id_list)
filtered_tag_scores["relevance"] = filtered_tag_scores["relevance"].apply(lambda x: 0 if x<0.2 else x)
#filtered tag-name df
filtered_tag_info_df = TagInfo.df

#----Merging movie-tagscores-taginfo dataframes----->
merge1 = pd.merge(filtered_tag_scores, filtered_movie_df, on="movie_id", how="inner")
merge1 = merge1[["movie_id", "name","year","tag_id","relevance" ]]

merge2 = pd.merge(merge1, filtered_tag_info_df, on="tag_id", how="inner")
merged = merge2[["movie_id", "name","year", "tag_id","tag_name", "relevance"]]


table= pd.pivot_table(merged, values="relevance", index=["movie_id", "name"], columns=["tag_name"])


ndf = pd.merge(filtered_movie_df[["movie_id","name", "year"]], table, on="movie_id", how="inner" )
ndf = ndf.round(2)
ndf.to_csv("/home/jb/Projects/Github/movielens/filtered-data/mini_tags_df.csv", index=False)

ndf.head()
















#################################################################################
#FOR AWS DB
from tqdm import tqdm
def get_pickle(file_dir, compress=False):
    import _pickle as pickle
    if compress:
        import bz2
        with bz2.BZ2File(file_dir, "r") as f:
            file = pickle.load(f)
        return file
    with open(file_dir, "rb") as f:
        file = pickle.load(f)
    return file


def get_json(file_dir):
    import json
    with open(file_dir, "r") as f:
        file = json.load(f)
    return file

def save_json(file, file_dir):
    import json
    with open(file_dir, "w") as f:
        json.dump(file, f)
    print("Saved to:'{}'".format(file_dir))

filtered_movie_id_list = get_pickle("/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie_id_list.pickle")

m = Movie.objects.get(id=2571)

def get_movie_data(movie_id_list, save_path):
    qs = Movie.objects.filter(id__in=movie_id_list).only("imdb_rating", "data")
    great_dict = {}
    for m in tqdm(qs):
        md = {}
        if m.imdb_rating:
            md["imdb_rating"] = float(m.imdb_rating)
        else:
            md["imdb_rating"] = 0
        md["movie_id"] = m.id
        md["name"] = m.name
        md["year"] = m.year
        if m.data.get("Runtime"):
            #print("runtime", m.data.get("Runtime"))
            rnt = m.data.get("Runtime").strip().split(" ")[0]
            rnt = "".join(rnt.split(","))
            md["runtime"] = int(rnt)
        else:
            md["runtime"] = 0
        if m.data.get("imdbVotes"):
            #print("imdb-votes", m.data.get("imdbVotes"))
            md["imdb_votes"] = int(m.data.get("imdbVotes"))
        else:
            md["imdb_votes"] = 0
        if m.data.get("Metascore"):
            #print("metascore", m.data.get("Metascore"))
            md["metascore"] = int(m.data.get("Metascore"))
        else:
            md["metascore"] = 0
        cm = Crew.objects.filter(movie=m, job="d")
        if cm.count()==1:
            md["director_id"] =cm[0].person.id
        elif cm.count()>=1:
            md["director_id"] = "+".join([x.person.id for x in cm])
        else:
            md["director_id"] = "0"
        great_dict[str(m.id)] = md
    print("saving...")
    save_json(great_dict, save_path)


#get_movie_data(filtered_movie_id_list, "/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie-info-dict.json")
import json
movie_info = pickson.get_json("/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie-info-dict.json")
movie_info_j = [x for x in movie_info.values()]
movie_info_j = json.dumps(movie_info_j)

mdf_j = pd.read_json(movie_info_j, orient="records")
mdf_j = mdf_j[["movie_id", "name","year", "director_id", "imdb_rating", "imdb_votes"]]
mdf_j.head()

#DROP IF HAVE ZERO
df = mdf_j[["movie_id", "imdb_rating", "imdb_votes"]]
df = df[(df != 0).all(1)]
df = df[["movie_id", "imdb_rating", "imdb_votes"]]


merge_big = pd.merge(ndf, df, on="movie_id", how="inner")
old_cols = merge_big.columns
first_cols = list(old_cols[:3])
mid_cols = list(old_cols[-2:])
last_cols = list(old_cols[3:-2])
merge_big = merge_big[first_cols + mid_cols + last_cols]

merge_big.to_csv("/home/jb/Projects/Github/movielens/filtered-data/mini-tag-movie-info.csv", index=False)
