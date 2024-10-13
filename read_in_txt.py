import pandas as pd
import json
import os

"""
- this script builds a csv file for mommit and daddit data
- first 4 dataframes are build from the text files. Mommit comments & submissions and daddit comments & submissions
- match mommit submissions with its comments & daddit submissions with its comments based on parts of the hyperlink
    - order of comments only in timely manner not real hierarchy considered. In most cases that is the real hierarchy anyways
"""


def build_df(txt_category):
    """
    creates a dataframe of different txt file that are already grouped into mommit/daddit & submission/ comments
    Args:
        txt_categroy: for each category there is a list [] of relevant file names
    """
    data_mod=[]
    for txt in txt_category:
        file_path_modular= os.path.join(cwd, txt)
        with open(file_path_modular, "r") as file:
            data_mod.extend([json.loads(line) for line in file])
    df_mod=pd.DataFrame(data_mod)
    
    # add human readable time stamp
    df_mod["date_time"]=pd.to_datetime(df_mod['created_utc'], unit='s')

    return df_mod

def rel_submissions(df:pd.DataFrame()):
    """
    reduces the submissions dataframes to relevant columns and adds a category column
    Args:
        df: df to reduce to only keep relevant columns for submissions df
    """
    df_submissions= df[["title", "selftext", "author", "date_time","permalink",  "id", "num_comments", "subreddit", "url", "score", "author_flair_text", "edited", "created_utc"]] # added score
    df_submissions["category"]="submissions" # add a column with category = submissions
    return df_submissions

def rel_comments(df:pd.DataFrame()):
    """
    reduces the comments dataframes to relevant columns and adds a category column
    Args:
        df: df to reduce to only keep relevant columns for comments df
    """
    df_comments= df[["body", "author","date_time", "parent_id", "link_id", "is_submitter", "permalink", "id", "subreddit", "score", "author_flair_richtext", "author_flair_template_id", "author_flair_text", "edited", "created_utc"]] # added id and score
    df_comments["category"]="comments" # add a column with category comments
    return df_comments


def submissions_comments_match(df_submissions:pd.DataFrame(), df_comments:pd.DataFrame(), lst_permalink):
    """
    This function combines submissions with comments for mommit and daddit into one dataframe. Permalink is what can bring the submission together
    with its respective comments. An empty df is initalized. A list with a permalink for each submission is needed. The function loops over the list
    and filters the df_comments for the respective permalink (startswith b/c for each comment the permalink has an indidual ending. But with 
    startswith i can grasp the relevant comments for the respective submission). The result is stored in comments. The submissions are also filtered for 
    the elements in the list. Comments and submissions are concated for each iteration and sorted by date_time. In the end this concated df is concated
    with the empty dataframe which succesivly with each iteration of the loop grows.
    Args:
        df_submissions: dataframe with submissions 
        df_comments: dataframe with comments
        lst_permalink: list with a permalink for each submission
    """
    pairs_df=pd.DataFrame() 
    for link in lst_permalink:
         comments= df_comments[df_comments["permalink"].str.startswith(link)]
         submissons= df_submissions[df_submissions["permalink"]==link]
         concat_subs_com= pd.concat([comments, submissons], axis=0, ignore_index=True).sort_values(by="date_time")
         pairs_df=pd.concat([pairs_df, concat_subs_com])

    return pairs_df



if __name__ == "__main__":
    # get current working directory
    # text files are in "filtered text files"
    cwd=os.getcwd()
    print(cwd)
    folder_name="filtered text files"
    folder_path= os.path.join(cwd, folder_name)
    files=os.listdir(folder_path)

    # get list of relevant text files
    daddit_submissions=[file for file in files if file.startswith('daddit_submissions')]
    daddit_comments=[file for file in files if file.startswith('daddit_comments')]
    mommit_submissions=[file for file in files if file.startswith('mommit_submissions')]
    mommit_comments=[file for file in files if file.startswith('mommit_comments')]

    # apply build_df function
    df_submissions_daddit=build_df(daddit_submissions)
    df_submissions_mommit=build_df(mommit_submissions)
    df_comments_daddit=build_df(daddit_comments)
    df_comments_mommit=build_df(mommit_comments)

    # reduce dfs to relevant columns
    df_submissions_daddit_short=rel_submissions(df_submissions_daddit)
    df_submissions_mommit_short=rel_submissions(df_submissions_mommit)
    df_comments_daddit_short=rel_comments(df_comments_daddit)
    df_comments_mommit_short=rel_comments(df_comments_mommit)

    # list with permalinks to submissions
    lst_permalinks_mommit= df_submissions_mommit_short["permalink"].values.tolist()
    lst_permalinks_daddit= df_submissions_daddit_short["permalink"].values.tolist()
    
    # based on permalink match submissions with respective comments
    subs_comments_mommit=submissions_comments_match(df_submissions_mommit_short, df_comments_mommit_short,lst_permalinks_mommit)
    subs_comments_daddit= submissions_comments_match(df_submissions_daddit_short, df_comments_daddit_short, lst_permalinks_daddit)

    # final csv files
    subs_comments_mommit.to_csv("mommit_subs_comments_final.csv", sep = ';') 
    subs_comments_daddit.to_csv("daddit_subs_comments_final.csv", sep=";")    