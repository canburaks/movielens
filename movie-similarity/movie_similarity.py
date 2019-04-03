import os
os.chdir('/home/jb/Projects/Github/movielens')
from managers import MovieManager, TagInfoManager, TagScoreManager, RatingManager
import pandas as pd
import numpy as np
from tqdm import tqdm
import pickson
from array import array

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

#filtered_movie_id_list = pickson.get_pickle("/home/jb/Projects/Github/movielens/filtered-data/movie-id-lists/movie_id_list.pickle")
#filtered_movie_id_list = sorted(filtered_movie_id_list)

def filter_by_selected_movies( movie_id_list):
    user_id_list, rdf = filter_by_user_quantity(25)
    filtered_df = rdf[rdf["movie_id"].isin(movie_id_list)]
    return movie_id_list, filtered_df


#movie_id_list, new_df = filter_by_selected_movies(filtered_movie_id_list)


def create_movie_dictionary(df,movie_id_list, path):
    import json
    great_dict = {}
    for mid in tqdm(movie_id_list):
        movie_dict = {}
        mdf = df[df["movie_id"]==mid]
        mdf = mdf.iloc[:,[0,2]]
        mdf_json = json.loads(mdf.to_json(orient="records"))
        for record in mdf_json:
            tup = tuple(record.values())
            movie_dict[ str(tup[0])] = float(tup[1])
        great_dict[str(mid)] = movie_dict
    print("file is saving...")
    pickson.save_json(great_dict, path)

#create_movie_dictionary(new_df, movie_id_list, "/home/jb/Projects/Github/movielens/data/movie_dictionary.json")

#movie_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/movie_dictionary.json")


################################################################################3
#DATABASE
similarity_csv_path = "/home/jb/Projects/Github/movielens/movie-similarity/movie_similarity_db.csv"

movie_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/movie_dictionary.json")
user_dictionary = pickson.get_json("/home/jb/Projects/Github/movielens/data/user_dictionary.json")

movie_id_list = sorted([int(x) for x in movie_dictionary.keys()])



cols = ["movie_id_1", "movie_name_1", "movie_id_2", "movie_name_2", "common", "acs"]
cols_mini = ["movie_id_1", "movie_id_2", "common", "acs"]

def similarity_calculation(start, stop, min_common):
    csv_db = pd.read_csv(similarity_csv_path)
    total_movie_quantity = len(movie_id_list)
    dict_list = []
    for index in range(total_movie_quantity)[start : stop]:
        movie_id_1 = movie_id_list[index]
        movie_1 = movie_dictionary.get(str(movie_id_1))
        movie_1_users = movie_1.keys()

        for movid2 in tqdm(range(index + 1, len(movie_id_list))):
            new_dict = {}
            movie_id_2 = movie_id_list[movid2]
            movie_2 = movie_dictionary.get(str(movie_id_2))
            movie_2_users = movie_2.keys()

            commons = set(movie_1_users).intersection(set(movie_2_users))

            if len(commons)<min_common:
                continue

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
            if bl*br!=0:
                similarity = round(top / ((bl*br)**0.5),3)
                if similarity<0.2:
                    continue
                new_dict["movie_id_1"] = movie_id_1
                new_dict["movie_id_2"] = movie_id_2
                new_dict["common"] = len(commons)
                new_dict["acs"] = similarity
                dict_list.append(new_dict)
            else:
                continue
    new_df = pd.DataFrame(data=dict_list)
    csv_db = csv_db.append(new_df, ignore_index=True)
    csv_db = csv_db[cols_mini]
    csv_db.to_csv(similarity_csv_path, index=False)

        
similarity_calculation(0,100,50)





movie_dictionary.get("1")

d1 = {"movie_id_1": 1, "movie_name_1": "asd", "movie_id_2": 2,
    "movie_name_2": "fdfssd", "common": 5, "acs": 0.45}
d2 = {"movie_id_1": 5, "movie_name_1": "asadassd", "movie_id_2": 9,
      "movie_name_2": "fsaddfssd", "common": 15, "acs": 0.145}

df = pd.DataFrame(data=[d1])
df2 = pd.DataFrame(data=[d2])
df = df.append(df2, ignore_index=True)

df3 = pd.DataFrame(data=[d1, d2])
df3
