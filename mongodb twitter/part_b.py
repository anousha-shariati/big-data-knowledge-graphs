import argparse
from pymongo import MongoClient


# helper to print results as a simple text table in the terminal
def print_table(title, rows, columns):
    print(f"\n{'='*70}\n{title}\n{'='*70}")
    if not rows:
        print("(no rows returned)")
        return
    widths = {c: max(len(c), max(len(str(r.get(c, ''))) for r in rows)) for c in columns}
    header = " | ".join(c.ljust(widths[c]) for c in columns)
    print(header)
    print("-" * len(header))
    for r in rows:
        print(" | ".join(str(r.get(c, '')).ljust(widths[c]) for c in columns))


# B-a: Emerging Hashtags (growth from one month to the next)

def query_b_a(db, m1_min=20, m2_min=40):
    # the task says "last two full weeks", but most of the tweets fall in a SAME ISO week, so give an empty result.
    # per the forum clarification I compare the two full MONTHS instead and keep the same thresholds (>=20 and >=40).
    pipeline = [
        {"$match": {"created_at": {"$ne": None}, "hashtags": {"$ne": []}}},
        # split the hashtags array so each hashtag is its own row
        {"$unwind": "$hashtags"},
        # bucket each tweet into the month it belongs to
        {"$addFields": {
            "month": {"$dateTrunc": {"date": "$created_at", "unit": "month"}}
        }},
        # count each hashtag per month
        {"$group": {"_id": {"hashtag": "$hashtags", "month": "$month"},
                    "cnt": {"$sum": 1}}},
        # rank the months so the newest is 1 and the one before is 2
        {"$setWindowFields": {
            "partitionBy": None,
            "sortBy": {"_id.month": -1},
            "output": {"monthRank": {"$denseRank": {}}}
        }},
        # keep only the two most recent months
        {"$match": {"monthRank": {"$in": [1, 2]}}},
        # put each hashtag's two month counts on a single row
        {"$group": {
            "_id": "$_id.hashtag",
            "month2_count": {"$sum": {"$cond": [{"$eq": ["$monthRank", 1]}, "$cnt", 0]}},
            "month1_count": {"$sum": {"$cond": [{"$eq": ["$monthRank", 2]}, "$cnt", 0]}},
        }},
        # apply the thresholds: earlier month >=20, later month >=40
        {"$match": {"month1_count": {"$gte": m1_min},
                    "month2_count": {"$gte": m2_min}}},
        # growth rate = later month / earlier month
        {"$addFields": {"growth_rate": {"$divide": ["$month2_count", "$month1_count"]}}},
        {"$sort": {"growth_rate": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "hashtag": "$_id",
                      "month1_count": 1, "month2_count": 1,
                      "growth_rate": {"$round": ["$growth_rate", 3]}}},
    ]
    return list(db.tweets_clean.aggregate(pipeline))


# B-b: Coordinated Link Bursts

def query_b_b(db):
    # find the same domain being posted by >=3 different users within a 60-second
    # window - a sign of coordinated / bot-like posting
    pipeline = [
        # only tweets that contain a link
        {"$match": {"has_link": True, "created_at": {"$ne": None}}},
        # split the urls array so each url is its own row
        {"$unwind": "$urls"},
        {"$match": {"urls.domain": {"$ne": None}}},
        # round the time down to a 60-second bucket
        {"$addFields": {
            "window_start": {"$dateTrunc": {"date": "$created_at",
                                            "unit": "second", "binSize": 60}}
        }},
        # group by domain + time bucket, collecting the distinct users and tweet ids
        {"$group": {
            "_id": {"domain": "$urls.domain", "window_start": "$window_start"},
            "users": {"$addToSet": "$user_id"},
            "tweet_ids": {"$addToSet": "$id_str"},
        }},
        {"$addFields": {"unique_users": {"$size": "$users"}}},
        # keep only buckets where at least 3 different users posted
        {"$match": {"unique_users": {"$gte": 3}}},
        {"$sort": {"unique_users": -1, "_id.window_start": 1}},
        # cap the output so it stays readable
        {"$limit": 25},
        {"$project": {
            "_id": 0,
            "domain": "$_id.domain",
            "window_start": "$_id.window_start",
            "unique_users": 1,
            "tweet_ids_sample": {"$slice": ["$tweet_ids", 5]},
        }},
    ]
    return list(db.tweets_clean.aggregate(pipeline))


# B-c: High-volume Near-Duplicates per User

def query_b_c(db, dup_min=5):
    # flag users who post the exact same clean_text 5+ times in one day (spam)
    pipeline = [
        {"$match": {"created_at": {"$ne": None},
                    "clean_text": {"$nin": [None, ""]}}},
        # round the time down to the day
        {"$addFields": {
            "day": {"$dateTrunc": {"date": "$created_at", "unit": "day"}}
        }},
        # group by user + day + text, and count how many times it repeats
        {"$group": {
            "_id": {"user_id": "$user_id", "day": "$day",
                    "clean_text": "$clean_text"},
            "dup_count": {"$sum": 1},
        }},
        # keep only groups repeated at least 5 times
        {"$match": {"dup_count": {"$gte": dup_min}}},
        # look up the user's screen name from the users collection
        {"$lookup": {"from": "users", "localField": "_id.user_id",
                     "foreignField": "user_id", "as": "u"}},
        {"$addFields": {"screen_name": {"$first": "$u.screen_name"}}},
        {"$sort": {"dup_count": -1}},
        {"$project": {
            "_id": 0,
            "user_id": "$_id.user_id",
            "screen_name": 1,
            "day": "$_id.day",
            "duplicate_text": "$_id.clean_text",
            "dup_count": 1,
        }},
    ]
    return list(db.tweets_clean.aggregate(pipeline))


# main: run the three queries and print each result table

def main():
    ap = argparse.ArgumentParser(description='COMP8210 A1 Section A Part B - analytics')
    ap.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    ap.add_argument('--db', default='comp8210')
    args = ap.parse_args()

    db = MongoClient(args.mongo_uri)[args.db]

    # B-a
    rows = query_b_a(db)
    print_table("B-a: Emerging Hashtags (month-over-month growth, March vs April 2016)",
                rows, ["hashtag", "month1_count", "month2_count", "growth_rate"])

    # B-b
    rows = query_b_b(db)
    print_table("B-b: Coordinated Link Bursts (>=3 distinct users in 60s)",
                rows, ["domain", "window_start", "unique_users", "tweet_ids_sample"])

    # B-c
    rows = query_b_c(db)
    print_table("B-c: High-volume Near-Duplicates per User (>=5 identical/day)",
                rows, ["user_id", "screen_name", "day", "dup_count", "duplicate_text"])


if __name__ == '__main__':
    main()