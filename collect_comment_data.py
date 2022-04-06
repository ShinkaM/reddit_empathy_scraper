import praw
from psaw import PushshiftAPI
import regex as re
import pandas as pd
from argparse import ArgumentParser
import time

import logging
import sys

def main():
    parser = ArgumentParser()
    parser.add_argument('-d', '--dir', help='Dirctory to save data', type=str)
    parser.add_argument('-l', '--lim', help='Number of reddit data to collect', type=int, default = 50000)
    parser.add_argument('-f', '--filename', help='Name for file', type=str)
    parser.add_argument('-s', '--subreddit', help='Name of sub to collect from', type=str, default='relationship_advice')
    parser.add_argument('-t', '--type', help='type of data to collect', type=str)
    

    opt = parser.parse_args()
    startTime = time.time()
    data = get_data(opt.lim, opt.subreddit, opt.type)
    print('total data filtered: ', len(data))
    data.to_csv(opt.dir + '/' + opt.filename)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))

def get_gender(sentence):
  # print('parsing ', sentence)

  fem_iam = '(((i am|i\'m) (also )*(a|an)) (\w*\s)?((woman|gal|female|girl|mom|sister|sis)[\w\s]*|mother[.!?]|(mother )[\w\s]*))'
  fem_as = '( (as) (a|an)) (\w*\s)?((woman|gal|female|girl|mom|sister|sis)[\w\s]*|mother[.!?]|(mother )[\w\s]*)'
  fem_ga = '(my|My|I|i|I\'m | me)\s?[\[|\(]([0-9][0-9](f)|(f)[0-9][0-9])[\]|\)]'

  male_iam = '((i am|i\'m) (also )*(a|an)) (\w*\s)?((dude|guy|male|boy|father|dad|bro|brother)|man[?.!]|man (\w*\s)?)'
  male_as = '( (as) (a|an)) (\w*\s)?((dude|guy|male|boy|father|dad|bro|brother)|man[?.!]|(man[\s,])[\w\s]*)'
  male_ga = '(my|My|I|i|I\'m | me)\s?[\[|\(]([0-9][0-9](m)|(m)[0-9][0-9])[\]|\)]'
  female_pattern = re.compile("(%s|%s|%s)" % (fem_iam, fem_as, fem_ga), re.IGNORECASE)
  male_pattern = re.compile("(%s|%s|%s)" % (male_iam, male_as, male_ga), re.IGNORECASE)
  if bool(re.search(male_pattern, sentence)):
    return 'M'
  elif bool(re.search(female_pattern, sentence)):
    return 'F'
  return None

def get_age(sentence):
  re_age_gender = '.*?(my|My|I|i|I\'m | me)\s?[\[|\(]([0-9][0-9](f|m)|(f|m)[0-9][0-9])[\]|\)]'
  re_age_stmt = '.*?(i am|i\'m) (\\d+) (years|yrs|yr) old[^e].*?'
  # re_age = '(f|m)?([0-9][0-9])'

  match_1 = re.match(re_age_gender, sentence)
  match_2 = re.match(re_age_stmt, sentence)
  age = -1
  if bool(match_1):
    if bool(re.match('[0-9][0-9]', match_1.groups()[1])):
      age = re.match('[0-9][0-9]', match_1.groups()[1]).group(0)
    elif bool(re.match('(f|m)?([0-9][0-9])', match_1.groups()[1])):
      age = re.match('(f|m)?([0-9][0-9])', match_1.groups()[1]).group(2)
  elif bool(match_2):
    age = match_2.groups()[1]
  return int(age)

def get_data(lim, sub, type):

    api = PushshiftAPI()

    # lim = 50000

    query = list(api.search_comments(subreddit = sub,
                                            filter=['id','parent_id','permalink','author', 'title', 
                                                    'subreddit','body','num_comments','score'],
                                            limit=lim
                                          ))
    df = pd.DataFrame(query)
    print('total data collected: ', len(df))
    if type == 'gender':

      ids = []
      genders = []
      ages = []
      usernames = []
      body = []
      debug = []
      for idx, row in df.iterrows():
          sub_id = row['id']
          if isinstance(row['body'], str):
              gender = get_gender(row['body'])
              if gender:
                ids.append(sub_id)
                genders.append(gender)
                body.append(row['body'])
                usernames.append(row['author'])
      # print(df['id'], 'ferfe', len(df))
      df_user = pd.DataFrame(list(zip(ids, usernames, genders, body)), columns = ['id', 'user', 'gender', 'body'])
      return df_user

    elif type == 'age':
      ids = []
      titles = []
      genders = []
      ages = []
      usernames = []
      body = []
      for idx, row in df.iterrows():
          sub_id = row['id']
          if isinstance(row['title'], str) and isinstance(row['selftext'], str):
            age = get_age(row['title'])
            if age > 0:
                # print(gender)
                ids.append(sub_id)
                ages.append(age)
                titles.append(row['title'])
                body.append(row['selftext'])
                usernames.append(row['author'])
            else:
                age = get_age(row['selftext'])
                if age > 0:
                # print(age)
                  ids.append(sub_id)
                  ages.append(age)
                  titles.append(row['title'])
                  body.append(row['selftext'])
                  usernames.append(row['author'])

      df_user = pd.DataFrame(list(zip(ids, usernames, ages, titles, body)), columns = ['id', 'user', 'age', 'title', 'body'])
      return df_user

if __name__ == '__main__':
    main()
