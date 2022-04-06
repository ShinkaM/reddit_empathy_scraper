import praw
from psaw import PushshiftAPI
import regex as re
import pandas as pd
from argparse import ArgumentParser
import time
import logging

def main():
    parser = ArgumentParser()
    parser.add_argument('-d', '--dir', help='Dirctory to save data', type=str)
    parser.add_argument('-l', '--lim', help='Number of reddit data to collect', type=int, default = 50000)
    parser.add_argument('-s', '--subreddit', help='Name of sub to collect from', type=str, default='relationship_advice')
    parser.add_argument('-t', '--type', help='type of data to collect', type=str)
    

    opt = parser.parse_args()
    logging.basicConfig(filename='log/' + opt.subreddit + '.log', encoding='utf-8', level=logging.DEBUG, filemode='w',)

    startTime = time.time()
    logging.info('Collecting ' + str(opt.lim) + ' lines from ' + opt.subreddit)
    data = get_data(opt.lim, opt.subreddit, opt.type, logging)

    logging.info('total filtered: ' + str(len(data)))
    data.to_csv(opt.dir + '/' + 'data_' + str(opt.lim) + '_' + opt.subreddit + '.csv')
    executionTime = (time.time() - startTime)
    logging.info('Execution time in seconds: ' + str(executionTime))

def get_gender(sentence):
  # print('parsing ', sentence)

  fem_iam = '(((i am|i\'m|i was) (also )*(a|an)) (\w*\s)?((woman|gal|female|girl|mom|sister|sis)|mother[.!?]))'
  fem_as = '((^as| as) (a|an)) (\w*\s)?((woman|gal|female|girl|mom|sister|sis)|mother[.!?,\s])'
  fem_ga = '(my|My|I|i|I\'m | me)\s?[\[|\(]([0-9][0-9](f)|(f)[0-9][0-9])[\]|\)]'

  male_iam = '((i am|i\'m|i was) (also )*(a|an)) (\w*\s)?((dude|guy|male|boy|father|dad|bro|brother)|man[?.!\s])'
  male_as = '((^as| as) (a|an)) (\w*\s)?((dude|guy|male|boy|father|dad|bro|brother)|man[?.!,\s])'
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

def get_data(lim, sub, type, logging):

    api = PushshiftAPI()

    # lim = 50000

    query = (api.search_submissions(
                                subreddit=sub,
                                filter=['id','author', 'title', 'selftext'],
                                limit=lim))

    submissions = list()
    for element in query:
        submissions.append(element.d_)
    # print("total data collected: ", len(submissions))
    logging.info("total data collected: "+ str(len(submissions)))
    df = pd.DataFrame(submissions)
  
    if type == 'gender':

      ids = []
      titles = []
      genders = []
      ages = []
      # comments = []
      usernames = []
      body = []
      for idx, row in df.iterrows():
          logging.info(idx)
          sub_id = row['id']
          if isinstance(row['title'], str) and isinstance(row['selftext'], str):
            gender = get_gender(row['title'])
            if gender:
                # print(gender)
                ids.append(sub_id)
                genders.append(gender)
                titles.append(row['title'])
                body.append(row['selftext'])
                usernames.append(row['author'])
            else:
                gender = get_gender(row['selftext'])
                if gender:
                # print(gender)
                  ids.append(sub_id)
                  genders.append(gender)
                  titles.append(row['title'])
                  body.append(row['selftext'])
                  usernames.append(row['author'])

      df_user = pd.DataFrame(list(zip(ids, usernames, genders, titles, body)), columns = ['id', 'user', 'gender', 'title', 'body'])
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
