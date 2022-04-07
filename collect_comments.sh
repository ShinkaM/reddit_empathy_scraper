# python3 rel_advice_collect.py -d ../../../../local/reddit_demographic/ -l 10000 -f data_abuse_gender10000.csv -t gender -s abuse
# python3 rel_advice_collect.py -d ../../../../local/reddit_demographic/ -l 10000 -f data_anxiety_gender10000.csv -t gender -s anxiety
# python3 rel_advice_collect.py -d ../../../../local/reddit_demographic/ -l 10000 -f data_depression_gender10000.csv -t gender -s depression
# python3 rel_advice_collect.py -d ../../../../local/reddit_demographic/ -l 10000 -f data_domesticviolence_gender10000.csv -t gender -s domesticviolence
# python3 rel_advice_collect.py -d ../../../../local/reddit_demographic/ -l 10000 -f data_bullying_gender10000.csv -t gender -s bullying
# python3 rel_advice_collect.py -d ../../../../local/reddit_demographic/ -l 10000 -f data_mentalhealth_gender10000.csv -t gender -s mentalhealth

python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_rel_advice_10000.csv -s relationship_advice
python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_aita_10000.csv -s aita
python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s relationships

python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s abuse
python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s anxiety
python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s depression
python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s domesticviolence
python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s bullying

python3 collect_comment_data.py -d ../../../../local/reddit_demographic/ -l 10000 -t gender -f comment_relationships_10000.csv -s mentalhealth



