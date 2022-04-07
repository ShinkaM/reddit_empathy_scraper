# %%
from socket import CAN_RAW
import praw
import pandas as pd
import os
import datetime as dt
import time
import logging
from argparse import ArgumentParser

def main():
    parser = ArgumentParser()
    parser.add_argument('-c', '--category', help='Category of data', type=str)
    opt = parser.parse_args()
    start_time = time.time()
    datadir = '../../../../local/reddit_demographic/' + opt.category + '/'
    os.chdir(datadir)
    get_data(opt.category)
    logging.basicConfig(filename='comments/log/' + opt.category + '.log', encoding='utf-8', level=logging.DEBUG, filemode='w',)
    executionTime = (time.time() - start_time)
    logging.info('Execution time in seconds: ' + str(executionTime))

def get_data(category):

    reddit = praw.Reddit(client_id='460necGe04OkzTt0i9cWGg', client_secret='zuvpweGNzmGaCUjrVNDi2JyQ36LEPg', user_agent='emphathy_aita')

    datadir = '../../../../local/reddit_demographic/' + category + '/'

    df = pd.read_csv('all_' + category + '_data.csv', index_col = None)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    comment_path = datadir + 'comments/'

    submissions_csv_path = category + '-submissions.csv'

    submissions_data_dict = {}
    for row in df.itertuples():
        i = row[1]
        # print(i)
        submissions_dict = {
            "id" : [],
            "score" : [],
            "num_comments": [],
            "created_utc" : [],
            'subreddit':[]
        }
        
        submission_praw = reddit.submission(id=i)

        submissions_dict["id"] = submission_praw.id
        submissions_dict["score"] = submission_praw.score
        submissions_dict["num_comments"] = submission_praw.num_comments
        submissions_dict["created_utc"] = submission_praw.created_utc
        submissions_dict["subreddit"] = str(submission_praw.subreddit)


        submission_comments_csv_path = category + '-submission_' + i + '-comments.csv'
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
        comment_df.to_csv(comment_path + '/' + submission_comments_csv_path,
                                                            index=False)
        # pd.DataFrame(submissions_dict).to_csv(comment_path + '/' + submissions_csv_path,
        #                                             index=False)    

        submissions_data_dict[i] = submissions_dict
    data_meta = pd.DataFrame(submissions_data_dict).T
    data_meta.to_csv(comment_path + '/' + submissions_csv_path,
                                                            index=False)

if __name__ == '__main__':
    main()
