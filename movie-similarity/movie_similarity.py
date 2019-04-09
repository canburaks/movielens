from functools import reduce
import os
os.chdir('/home/jb/Projects/Github/movielens')
from managers import MovieManager, TagInfoManager, TagScoreManager, RatingManager
import pandas as pd
import numpy as np
from tqdm import tqdm
import pickson
from array import array
pd.set_option('display.max_columns', 800)
pd.set_option('display.max_rows', 800)
#valid_rating_file = "/home/jb/Projects/Github/movielens/data/vratings.csv"

#USER-QUANTITY SERIES
#uq = pd.read_csv("/home/jb/Projects/Github/movielens/filtered-data/ratings/user-quantity.csv")


def filter_by_user_quantity(minimum):
    df = pd.read_csv("/home/jb/Projects/Github/movielens/data/vratings.csv")
    uq = pd.read_csv("/home/jb/Projects/Github/movielens/filtered-data/ratings/user-quantity.csv")
    user_id_list = list(set(uq[uq["quantity"]>=minimum]["user_id"].tolist()))
    filtered_df = df[df["user_id"].isin(user_id_list)]
    return sorted(user_id_list), filtered_df

#User id list and rating dataframe of quantity based filtering
#user_id_list, rdf = filter_by_user_quantity(25)


def create_user_dictionary(df, user_id_list, path):
    great_dict = {}
    for uid in tqdm(user_id_list):
        user_dict = {}
        udf = df[df["user_id"]==uid]
        #mean
        user_dict["mean"] = round(udf["rating"].agg(np.mean),3)
        for i,r in udf.iterrows():
            movie_id = str(int(r[1]))
            rate = float(r[2])
            user_dict[movie_id] = rate
        great_dict[str(uid)] = user_dict
    print("file is saving...")
    pickson.save_json(great_dict, path)

#create_user_dictionary(rdf, user_id_list, "/home/jb/Projects/Github/movielens/data/user_dictionary.json")

#user_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/user_dictionary.json")
#########################################################################

filtered_movie_id_list = pickson.get_pickle("/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie_id_list_10k.pickle")
filtered_movie_id_list = sorted(filtered_movie_id_list)

def filter_by_selected_movies( movie_id_list):
    user_id_list, rdf = filter_by_user_quantity(25)
    filtered_df = rdf[rdf["movie_id"].isin(movie_id_list)]
    return movie_id_list, filtered_df


movie_id_list, new_df = filter_by_selected_movies(filtered_movie_id_list)

#for movie name appending to movie_dictionary
movie_df_for_name = pd.read_csv("/home/jb/Projects/Github/movielens/data/vmovies.csv")
mndf = movie_df_for_name[["movie_id", "name"]]

def create_movie_dictionary(df,movie_id_list, path):
    import json
    great_dict = {}
    for mid in tqdm(movie_id_list):
        movie_dict = {}
        try:
            name = mndf[mndf["movie_id"] == int(mid)].iloc[0, 1]
        except:
            name = ""
        movie_dict["name"] = name
        mdf = df[df["movie_id"]==mid]
        mdf = mdf.iloc[:,[0,2]]
        mdf_json = json.loads(mdf.to_json(orient="records"))
        for record in mdf_json:
            tup = tuple(record.values())
            movie_dict[ str(tup[0])] = float(tup[1])
        great_dict[str(mid)] = movie_dict
    print("file is saving...")
    pickson.save_json(great_dict, path)

create_movie_dictionary(new_df, movie_id_list, "/home/jb/Projects/Github/movielens/data/movie_dictionary.json")

#movie_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/movie_dictionary.json")


################################################################################3
#DATABASE
similarity_csv_path = "/home/jb/Projects/Github/movielens/movie-similarity/movie_similarity_db.csv"

movie_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/movie_dictionary.json")
user_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/user_dictionary.json")
movie_id_list = sorted([int(x) for x in movie_dictionary.keys()])



cols_mini = ["movie_id_1","movie_name_1", "movie_id_2","movie_name_2", "common","pearson", "acs"]


def similarity_calculation(start, stop, min_common):
    csv_db = pd.read_csv(similarity_csv_path)
    total_movie_quantity = len(movie_id_list)
    dict_list = []
    for index in tqdm(range(total_movie_quantity)[start : stop]):
        movie_id_1 = movie_id_list[index]
        movie_1 = movie_dictionary.get(str(movie_id_1))
        movie_name_1 = movie_1.get("name")

        m1_keys = list(movie_1.keys())
        m1_keys.remove("name")
        movie_1_users = m1_keys

        
        for movid2 in range(index + 1, len(movie_id_list)):
            new_dict = {}
            movie_id_2 = movie_id_list[movid2]
            movie_2 = movie_dictionary.get(str(movie_id_2))
            movie_name_2 = movie_2.get("name")

            m2_keys = list(movie_2.keys())
            m2_keys.remove("name")
            movie_2_users = m2_keys

            commons = set(movie_1_users).intersection(set(movie_2_users))

            if len(commons)<min_common:
                continue
            ## ----------ACS--------------------
            acs_top = 0
            acs_bl = 0
            acs_br = 0
            for u in commons:
                ubar = user_dictionary.get(str(u)).get("mean")
                ui = movie_1.get(str(u))
                uj = movie_2.get(str(u))

                acs_top += (ui - ubar)*(uj - ubar)
                acs_bl += (ui - ubar)**2
                acs_br += (uj - ubar)**2
            if acs_bl * acs_br!=0:
                acs = round(acs_top / ((acs_bl*acs_br)**0.5),3)
            else:
                acs = -1
            #------PEARSON------------------
            
            m1_values = list(movie_1.values())
            m1_values.remove(movie_1.get("name"))

            m2_values = list(movie_2.values())
            m2_values.remove(movie_2.get("name"))

            ibar = reduce(lambda x, y: x+y, m1_values)/(len(movie_1)-1)
            jbar = reduce(lambda x, y: x+y, m2_values)/(len(movie_2)-1)
            pearson_top = 0
            pearson_bl = 0
            pearson_br = 0

            for u in commons:
                ui = movie_1.get(str(u))
                uj = movie_2.get(str(u))

                pearson_top += (ui - ibar)*(uj - jbar)
                pearson_bl += (ui - ibar)**2
                pearson_br += (uj - jbar)**2
            if pearson_bl * pearson_br != 0:
                pearson = round(pearson_top / ((pearson_bl*pearson_br)**0.5), 3)
            else:
                pearson = -1
            if pearson>=0.4 or acs>=0.4:
                new_dict["pearson"] = pearson
                new_dict["acs"] = acs

                new_dict["movie_id_1"] = movie_id_1
                new_dict["movie_name_1"] = movie_name_1

                new_dict["movie_id_2"] = movie_id_2
                new_dict["movie_name_2"] = movie_name_2

                new_dict["common"] = len(commons)
                
                dict_list.append(new_dict)
            else:
                continue

    new_df = pd.DataFrame(data=dict_list)
    csv_db = csv_db.append(new_df, ignore_index=True)
    csv_db = csv_db[cols_mini]
    csv_db.to_csv(similarity_csv_path, index=False)

        
similarity_calculation(0,1, 200)


########################################################3
def compare_movies_acs(mid_1, mid_2):
    movie_1 = movie_dictionary.get(str(mid_1))
    movie_2 = movie_dictionary.get(str(mid_2))
    commons = list(set(movie_1.keys()).intersection(set(movie_2.keys())))
    top = 0
    bl = 0
    br = 0
    for u in commons:
        ubar = user_dictionary.get(str(u)).get("mean")
        ui = movie_1.get(str(u))
        uj = movie_2.get(str(u))

        top += (ui - ubar)*(uj - ubar)
        bl += (ui - ubar)**2
        br += (uj - ubar)**2
    similarity = round(top / ((bl*br)**0.5), 3)
    return {"movie_id_1": mid_1, "movie_id_2": mid_2, "commons": len(commons), "acs": similarity}


def compare_movies_pearson(mid_1, mid_2):
    movie_1 = movie_dictionary.get(str(mid_1))
    movie_2 = movie_dictionary.get(str(mid_2))
    commons = list(set(movie_1.keys()).intersection(set(movie_2.keys())))

    ibar = reduce(lambda x, y: x+y, movie_1.values())/len(movie_1)
    jbar = reduce(lambda x, y: x+y, movie_2.values())/len(movie_2)
    top = 0
    bl = 0
    br = 0

    for u in commons:
        ui = movie_1.get(str(u))
        uj = movie_2.get(str(u))

        top += (ui - ibar)*(uj - jbar)
        bl += (ui - ibar)**2
        br += (uj - jbar)**2
    similarity = round(top / ((bl*br)**0.5), 3)
    return {"movie_id_1": mid_1, "movie_id_2": mid_2, "commons": len(commons), "acs": similarity}

#################################################################################################3
#GET MOVIE-SIMILARITY DF

import pandas as pd
class MovSim():
    def __init__(self, path):
        self.df = pd.read_csv(path)
    
    def get_movie(self, movie_id,  similarity=0, commons=0):
        movie_df = self.df[(self.df["movie_id_1"] == movie_id) | (self.df["movie_id_2"] == movie_id)]
        filtered =  movie_df[ (movie_df["acs"]>=similarity) & (movie_df["common"]>=commons)]
        return filtered.sort_values(by=["acs"], ascending=False)


    def compare(self, movie_id_1, movie_id_2):
        m1, m2 = sorted([movie_id_1, movie_id_2])
        return self.df[ (self.df["movie_id_1"]==m1) & (self.df["movie_id_2"]==m2) ]

ms = MovSim("/home/jb/Projects/Github/movielens/movie-similarity/movie_similarity_db.csv")
