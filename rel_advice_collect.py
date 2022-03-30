import praw
from psaw import PushshiftAPI
import regex as re
import pandas as pd
from argparse import ArgumentParser
import time

def main():
    parser = ArgumentParser()
    parser.add_argument('-d', '--dir', help='Dirctory to save data', type=str)
    parser.add_argument('-l', '--lim', help='Number of reddit data to collect', type=int, default = 50000)
    parser.add_argument('-f', '--filename', help='Name for file', type=str)
    opt = parser.parse_args()
    startTime = time.time()
    data = get_data(opt.lim)
    print(len(data))
    data.to_csv(opt.dir + '/' + opt.filename)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))

def get_gender(m_reg, f_reg, sentence):
  # print('parsing ', sentence)
  if bool(re.search(m_reg, sentence)):
    return 'M'
  elif bool(re.search(f_reg, sentence)):
    return 'F'
  return ''

def get_data(lim):

    api = PushshiftAPI()

    # lim = 50000

    query = (api.search_submissions(
                                subreddit='relationship_advice',
                                filter=['id','author', 'title', 'selftext'],
                                limit=lim))

    submissions = list()
    for element in query:
        submissions.append(element.d_)
    print("total data collected ", len(submissions))
    df = pd.DataFrame(submissions)


    fem_iam = '(((i am|i\'m) (also )*(a|an)) (\w*\s)?((woman|gal|female|girl|mom|sister|sis)[\w\s]*|mother[.!?]|(mother )[\w\s]*))'
    fem_as = '((as) (a|an)) (\w*\s)?((woman|gal|female|girl|mom|sister|sis)[\w\s]*|mother[.!?]|(mother )[\w\s]*)'
    fem_ga = '(my|My|I|i|I\'m | me)\s?[\[|\(]([0-9][0-9](f)|(f)[0-9][0-9])[\]|\)]'

    male_iam = '((i am|i\'m) (also )*(a|an)) (\w*\s)?((dude|guy|male|boy|father|dad|bro|brother)|man[?.!]|man (\w*\s)?)'
    male_as = '((as) (a|an)) (\w*\s)?((dude|guy|male|boy|father|dad|bro|brother)|man[?.!]|(man[\s,])[\w\s]*)'
    male_ga = '(my|My|I|i|I\'m | me)\s?[\[|\(]([0-9][0-9](m)|(m)[0-9][0-9])[\]|\)]'
    female_pattern = re.compile("(%s|%s|%s)" % (fem_iam, fem_as, fem_ga), re.IGNORECASE)
    male_pattern = re.compile("(%s|%s|%s)" % (male_iam, male_as, male_ga), re.IGNORECASE)



    ids = []
    titles = []
    genders = []
    ages = []
    # comments = []
    usernames = []
    body = []
    for idx, row in df.iterrows():
        sub_id = row['id']
        if row['title']:
          gender = get_gender(male_pattern, female_pattern, row['title'])
          if gender:
              # print(gender)
              ids.append(sub_id)
              genders.append(gender)
              titles.append(row['title'])
              body.append(row['selftext'])
              usernames.append(row['author'])
        elif row['selftext']:
          gender = get_gender(male_pattern, female_pattern, row['selftext'])
          if gender:
              # print(gender)
              ids.append(sub_id)
              genders.append(gender)
              titles.append(row['title'])
              body.append(row['selftext'])
              usernames.append(row['author'])

    df_user = pd.DataFrame(list(zip(ids, usernames, genders, titles, body)), columns = ['id', 'user', 'gender', 'title', 'body'])
    return df_user

if __name__ == '__main__':
    main()
