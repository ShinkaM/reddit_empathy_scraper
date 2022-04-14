# %%
import praw
import pandas as pd
import os
import datetime as dt
import time
from prawcore.exceptions import Forbidden, NotFound
import regex as re
import pathlib
from argparse import ArgumentParser
import logging

# %% [markdown]
# # how to format data
# - category
#     - comments
#         - subreddit+num_data
#             id+csv
def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--category', help='Category of data', type=str)
    parser.add_argument('-l', '--lim', help='Num of data collection to use', type=str, default=20000)
    opt = parser.parse_args()
    start_time = time.time()
    datadir = '../../../../local/reddit_demographic/' + opt.category + '/'
    os.chdir(datadir)
    print(os.getcwd())
    comment_path = datadir + '/comments/'
    pathlib.Path(comment_path).mkdir(parents=True, exist_ok=True) 
    pathlib.Path(comment_path + '/log/').mkdir(parents=True, exist_ok=True) 

    get_data(opt.lim)
    logging.basicConfig(filename='comments/log/' + opt.category + '.log', encoding='utf-8', level=logging.DEBUG, filemode='w',)
    executionTime = (time.time() - start_time)
    # print(len())
    logging.info('Execution time in seconds: ' + str(executionTime))

def get_data(lim):
    reddit = praw.Reddit(client_id='460necGe04OkzTt0i9cWGg', client_secret='zuvpweGNzmGaCUjrVNDi2JyQ36LEPg', user_agent='emphathy_aita')

    reddit = praw.Reddit(client_id='460necGe04OkzTt0i9cWGg', client_secret='zuvpweGNzmGaCUjrVNDi2JyQ36LEPg', user_agent='emphathy_aita')
    datadir = os.getcwd()

    total_comments = 0
    for sub in  os.listdir():#for each subreddit comments
        if 'comments' not in sub:
            if 'data_' + str(lim)  in sub:
                subreddit  = re.sub('data_' + str(lim) + '_', '', sub)
                subreddit = re.sub('.csv', '', subreddit)
                subreddit_data = datadir + '/' + sub
                sub_path = subreddit + '_' + str(lim)
                pathlib.Path(sub_path).mkdir(parents=True, exist_ok=True) 

                df = pd.read_csv(subreddit_data, index_col = None)
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                submissions_csv_path = subreddit + '-submissions.csv'
                    
                submissions_data_dict = {}
                for row in df.itertuples():
                    i = row[1]
                    submissions_dict = {
                        "id" : [],
                        "score" : [],
                        "num_comments": [],
                        "created_utc" : [],
                        'subreddit':[]
                    }
                    try:
                        submission_praw = reddit.submission(id=i)

                        submissions_dict["id"] = submission_praw.id
                        submissions_dict["score"] = submission_praw.score
                        submissions_dict["num_comments"] = submission_praw.num_comments
                        submissions_dict["created_utc"] = submission_praw.created_utc
                        submissions_dict["subreddit"] = str(submission_praw.subreddit)

                        total_comments += submission_praw.num_comments
                        submission_comments_csv_path = sub_path + '/' + i + '_comments.csv'
                        submission_comments_dict = {
                            "comment_id" : [],
                            "comment_parent_id" : [],
                            "comment_body" : [],
                            "comment_link_id" : [],
                        }

                        submission_praw.comments.replace_more(limit=None)
                        # for each comment in flattened comment tree
                        for comment in submission_praw.comments.list():
                            submission_comments_dict["comment_id"].append(comment.id)
                            submission_comments_dict["comment_parent_id"].append(comment.parent_id)
                            submission_comments_dict["comment_body"].append(comment.body)
                            submission_comments_dict["comment_link_id"].append(comment.link_id)

                        comment_df = pd.DataFrame(submission_comments_dict)
                        comment_df.to_csv(submission_comments_csv_path,index=False)
                    # pd.DataFrame(submissions_dict).to_csv(comment_path + '/' + submissions_csv_path,
                    #                                             index=False)    

                        submissions_data_dict[i] = submissions_dict
                        # break
                    except Forbidden:
                        submissions_data_dict[i] = submissions_dict

                    except NotFound:
                        submissions_data_dict[i] = submissions_dict
                data_meta = pd.DataFrame(submissions_data_dict).T
                data_meta.to_csv(sub_path + '/' + submissions_csv_path,
                                                                        index=False)
    # break
    print(str(total_comments) + ' comments collected')

    # pathlib.Path(comment_path).mkdir(parents=True, exist_ok=True) 

if __name__ == '__main__':
    main()
