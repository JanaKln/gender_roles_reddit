import pandas as pd
from langdetect import detect
from datetime import datetime, timedelta
import numpy as np

"""
Data cleaning
"""

def data_quality_check(df_orig:pd.DataFrame()):
    """"
    This function tests the original dataframe:
        - for duplicates
        - checks submissions for cases where selftext & title were later on deleted
        - checks for removed comments
        - checks for deleted or removed comments
    """
    # check for duplicates
    duplicates = df_orig[df_orig.duplicated()]
    if len(duplicates)>0:
        print("Attention: Duplicates in dataframe")

    # check for submissions with deleted selftext & title (if only selftext is deleted but title remains the post is still online)
    deleted_submissions= df_orig[(df_orig["selftext"]=="[deleted]") & (df_orig["title"]=="[deleted]")] # empty dataframe
    if len(deleted_submissions)>0:
        print("Attention: deleted selftext & title") # drop them

    # removed by moderator -> selftext says [removed]
    removed_selftext=df_orig[df_orig["selftext"]=="[removed]"]
    if len(removed_selftext)>0:
        print("Attention: the moderator removed submission(s). Decide how to handle these cases, as the title is still there")

    # removed or deleted comments (no other information left --> drop later)
    comments_to_drop= df_orig[(df_orig["body"] == "[removed]") | (df_orig["body"] == "[deleted]")]

    if len(comments_to_drop)>0:
        drop_comments=clean_comments(df_orig)

    return drop_comments

def clean_comments(df_orig:pd.DataFrame()):
    """
    This function removes deleted and removed rows from the original df
    Args: original dataframe (mommit or daddit)
    returns: clean df without deleted or removed comments
    """
    df_drop=df_orig[(df_orig["body"] == "[removed]") | (df_orig["body"] == "[deleted]")]

    # merge the dataframe with rows to be dropped and the original dataframe
    merged = pd.merge(df_orig, df_drop, how='left', indicator=True)

    # create dataframe that contains the rows to be removed (that are in the original and the dropped df)
    removed_rows = merged[merged['_merge'] == 'both']
    
    # create a boolean mask for the rows to be removed
    removed_mask = df_orig.isin(removed_rows.to_dict('list')).all(axis=1)
    
    # use  boolean mask to remove the rows from df_orig
    clean_df = df_orig[~removed_mask]

    return clean_df


def get_month_submissions(df_orig:pd.DataFrame(), year:int, month:int):
    """
    This function creates a dataframe of the submission for a month of interest
    Args:
        df_orig: dataframe to filter for a respective month
        year: year of interest
        month: month of interest
    returns:
        df_month: dataframe filtered to month and year of interest
    
    """
    # create timestamps for start and end of the month of interest
    start_date = datetime(year=year, month=month, day=1)
    end_date = start_date + timedelta(days=pd.Period(start_date, freq='M').days_in_month-1, hours=23, minutes=59, seconds=59)
    
    # create date_time object and filter for submissions
    df_orig["date_time_dt"]= pd.to_datetime(df_orig['date_time'])
    submissions = df_orig[df_orig['category'] == 'submissions']

    # filter for time period of interst
    df_month = submissions[(submissions['date_time_dt'] >= start_date) & (submissions['date_time_dt'] <= end_date)]
    
    return df_month

def get_month(df_orig:pd.DataFrame(), year: int, month:int):
    """
    This function creates a dataframe for a respective month of interest from an input dataframe (mommit or daddit) for submissions & comments

    """
    # create timestamps for start and end of the month of interest
    start_date = datetime(year=year, month=month, day=1)
    end_date = start_date + timedelta(days=pd.Period(start_date, freq='M').days_in_month-1, hours=23, minutes=59, seconds=59)
    
    df_orig["date_time_dt"]= pd.to_datetime(df_orig['date_time'])
    
    # filter for time period of interest
    df_month_whole_df = df_orig[(df_orig['date_time_dt'] >= start_date) & (df_orig['date_time_dt'] <= end_date)]
    
    return df_month_whole_df

def detect_language(text):
    """
    function that detects the language of a text
    """
    try:
        lang = detect(text)
    except:
        lang = 'unknown'
    return lang


def title_selftext_merge(df_orig):
    """
    This function combines the selfext and title columns into one
    """
    df_orig["whole_text"] = df_orig.apply(lambda x: x["title"] + "\n" + x["selftext"] if not pd.isna(x["title"]) and not pd.isna(x["selftext"]) else x["title"] if not pd.isna(x["title"]) else x["selftext"], axis=1)
    
    # add detected language for each row
    df_orig['language'] = df_orig['title'].apply(detect_language)

    return df_orig #submissions

def clean_text_data(df_orig:pd.DataFrame):
    """
    this function applies some data cleaning    
    """
   
    # ar and fa, th should be removed
    invalid_lang = ['ar', 'fa', 'th']
    df_orig = df_orig[~df_orig['language'].isin(invalid_lang)]

    # replace deleted and removed with blank
    # escape character \ for squared brackets
    df_orig["whole_text"]=df_orig["whole_text"].str.replace("\[removed\]", "")
    clean_df= df_orig.copy()
    clean_df["whole_text"]=clean_df["whole_text"].str.replace("\[deleted\]", "")

    return clean_df

def hierarchy_label(df:pd.DataFrame(), level:int):
    """
    - The function is structured in 3 parts
    1) First i create a short version of the parent id that cuts the first 3 characters of the values.
    2) Then i create a variable hier1 which states that the comment is a direct response to a submission. In that cases, parent_id == link id. 
    3) Then for all other hierarchy levels, another logic is applied. Exemple of hierarchy 2 (reply to comment that was posted directly under submission):
        the loop gets all ids for the rows where hier1==1 and saves it in search_value. For all rows that are of hier2, the id from rows with hier1 is equal
        to parent_id_short. The loop starts with hier2 and ends with the level inputted e.g. 10
    
    Args:
        df: mommit and daddit df
        level: how many levels should be found 
    """

    df["parent_id"] = df["parent_id"].astype(str)
    df["parent_id_short"]= df["parent_id"].apply(lambda x: x[3:] if not pd.isna(x) else x)

    # level=level --> müsste raus können
    df["hier1"]=np.where(df["parent_id"] == df["link_id"], 1, 0)

    for i in range(2, level):
        # Get id column for cases where comment hierarchy == i-1
        search_value = df.query(f"hier{i-1}=={i-1}")["id"]
        search_value = [str(x) for x in search_value.tolist()]

        # Create hierarchy column for level i
        df[f"hier{i}"] = np.where(df["parent_id_short"].isin(search_value), i, 0)

    return df


def add_info(df:pd.DataFrame()):
    """
    This function is designed to add further information to the dataframe. 

    Args:
        df = original data
    
    """
    # apply hierarchy label function
    clean_df_test=hierarchy_label(df, 16) # bis zum 15. level

    # add all hier columns to one complete columns
    hier_cols=[col for col in clean_df_test.columns if col.startswith("hier")]
    clean_df_test["test_hier_complete"]= clean_df_test[hier_cols].sum(axis=1)
    clean_df_test['test_hier_complete'].astype(int)

    # create a column "entity" that groups together one submission with its corresponding comments. To be able to group one entity use permalink
    # if category == comment, delete last /.../
    for idx, row in clean_df_test.iterrows():
        if row["category"] == "comments":
            permalink_parts = row["permalink"].split("/")
            permalink_parts[-2] = ""
            permalink_parts = [part for part in permalink_parts if part != ""]
            clean_df_test.at[idx, "permalink_short"] = "/"+ "/".join(permalink_parts) + "/"
        elif row["category"] == "submissions":
            clean_df_test.at[idx, "permalink_short"] = row["permalink"]
    
    # Image in submission yes or no
    clean_df_test['image'] = np.where((clean_df_test['url'].notna() & clean_df_test['url'].str.endswith('.jpg')), 1, 0)

    return clean_df_test


def last_cleaning(df:pd.DataFrame):
    # clean dataframes with duplicated texts (sometimes the same things by the same authors are posted several times)
    
    # Split the dataframe into submissions and comments
    submissions_df = df.loc[df['category'] == 'submissions']
    comments_df = df.loc[df['category'] == 'comments']

    # Apply drop_duplicates to the submissions_df only
    submissions_df = submissions_df.drop_duplicates(subset=['whole_text', 'author']).reset_index(drop=True)

    # drop comments with less than 10 words: to be sure to grasp relevant info and not sth like "Awww thank you"
    comments_df= comments_df[comments_df["body"].apply(lambda x: len(x.split())) >=10] 

    # 86 cases from duplicated submissions dropped, 7023 comments with less than 10 words dropped (mommit)
    final_df = pd.concat([submissions_df, comments_df], ignore_index=True) 

    # copy the body column (text column of comments)
    final_df["body_"]= final_df["body"]
    # put body to whole_text if whole_text is nan
    final_df['whole_text'] = final_df['whole_text'].fillna(final_df.pop('body'))

    ## Replace html tags with empty strings
    # &amp = ampersand is depicted as "&"" on Reddit
    final_df['whole_text'] = final_df['whole_text'].str.replace("\&amp;", "&") 
    # &gt; means that a sentence or part of another post/comment is referenced
    final_df['whole_text'] = final_df['whole_text'].str.replace("\&gt;", "") 
    # replace * with "" --> fat font
    final_df['whole_text'] = final_df['whole_text'].str.replace("*", "")

    ## drop duplicates where same person posts exact same thing in 5 min range - bots
    # Convert post_time to datetime format
    final_df['date_time'] = pd.to_datetime(final_df['date_time'])

    # Sort dataframe by post_time
    final_df.sort_values(by=["author", 'date_time'], inplace=True)

    # Calculate time difference between posts
    time_diff = final_df['date_time'].diff()

    # Find rows where author and text are the same and posted within 5 minutes of each other
    duplicates = final_df[(final_df.duplicated(subset=['author', 'whole_text'], keep=False)) & (time_diff <= pd.Timedelta('5 min'))]

    # Drop the duplicate rows
    final_df.drop(duplicates.index, inplace=True)

    # drop cases with author == AutoModerator
    final_df=final_df[final_df["author"] != "AutoModerator"]

    return final_df


if __name__ == "__main__":
    # read in csv files
    mommit=pd.read_csv('mommit_subs_comments_final.csv', sep = ';').iloc[:,1:]
    daddit=pd.read_csv('daddit_subs_comments_final.csv', sep = ';').iloc[:,1:]

    # add information (hierarchy level and image)
    mommit_add_info = add_info(mommit) 
    daddit_add_info=add_info(daddit)

    # data quality check
    mommit_check= data_quality_check(mommit_add_info)
    daddit_check=data_quality_check(daddit_add_info)

    # drop comments with [deleted] or [removed] in body
    mommit_clean_comment=clean_comments(mommit_check)   
    daddit_clean_comment=clean_comments(daddit_check)

    # merge title and selftext of submissions
    mommit_final = title_selftext_merge(mommit_clean_comment)
    daddit_final = title_selftext_merge(daddit_clean_comment)

    # clean submissions from languages or [deleted] or [removed] snippets
    mommit_final_2= clean_text_data(mommit_final)
    daddit_final_2=clean_text_data(daddit_final)

    # final steps for clean dataframe
    mommit_final_final= last_cleaning(mommit_final_2)
    daddit_final_final= last_cleaning(daddit_final_2)

    # sort submission with respective comments by time
    mommit_final_final=mommit_final_final.sort_values(["permalink_short", "date_time"])
    daddit_final_final=daddit_final_final.sort_values(["permalink_short", "date_time"])

    # final clean and preprocessed data --> input data for BERTopic
    mommit_final_final.to_csv("mommit_clean.csv", sep = ';') 
    daddit_final_final.to_csv("daddit_clean.csv", sep=";")
