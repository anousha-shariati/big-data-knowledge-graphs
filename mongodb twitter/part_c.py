import argparse
from pymongo import MongoClient, ASCENDING


def print_table(title, rows, columns):
    print(f"\n{'='*72}\n{title}\n{'='*72}")
    if not rows:
        print("(no rows returned)")
        return
    widths = {c: max(len(c), max(len(str(r.get(c, ''))) for r in rows)) for c in columns}
    header = " | ".join(c.ljust(widths[c]) for c in columns)
    print(header)
    print("-" * len(header))
    for r in rows:
        print(" | ".join(str(r.get(c, '')).ljust(widths[c]) for c in columns))


# C-a: Sock-puppet Signals

def build_user_day_hashtags(db):
    """Materialize a helper collection: one doc per (user, day) with the set of
    hashtags used that day, plus the user's location and url_domain for the join.
    """
    pipeline = [
        {"$match": {"created_at": {"$ne": None}, "hashtags": {"$ne": []}}},
        {"$addFields": {"day": {"$dateTrunc": {"date": "$created_at", "unit": "day"}}}},
        {"$unwind": "$hashtags"},
        {"$group": {"_id": {"user_id": "$user_id", "day": "$day"},
                    "tags": {"$addToSet": "$hashtags"}}},
        {"$lookup": {"from": "users", "localField": "_id.user_id",
                     "foreignField": "user_id", "as": "u"}},
        {"$addFields": {
            "user_id": "$_id.user_id",
            "day": "$_id.day",
            "location": {"$toLower": {"$first": "$u.location"}},
            "url_domain": {"$first": "$u.url_domain"},
            "screen_name": {"$first": "$u.screen_name"},
        }},
        {"$project": {"_id": 0, "u": 0}},
        {"$out": "user_day_hashtags"},
    ]
    db.tweets_clean.aggregate(pipeline, allowDiskUse=True)
    db.user_day_hashtags.create_index([("day", ASCENDING)])


def query_c_a(db, limit=25):
    """Self-join user_day_hashtags on the same day: pairs (a<b) who share a
    location OR url_domain and have >=3 hashtags in common that day.
    """
    build_user_day_hashtags(db)
    pipeline = [
        {"$lookup": {
            "from": "user_day_hashtags",
            "let": {"me": "$user_id", "d": "$day",
                    "loc": "$location", "dom": "$url_domain", "mytags": "$tags"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$day", "$$d"]},
                    {"$lt": ["$$me", "$user_id"]},
                    {"$or": [
                        {"$and": [{"$ne": ["$$loc", None]},
                                  {"$eq": ["$location", "$$loc"]}]},
                        {"$and": [{"$ne": ["$$dom", None]},
                                  {"$eq": ["$url_domain", "$$dom"]}]},
                    ]},
                    {"$gte": [{"$size": {"$setIntersection": ["$tags", "$$mytags"]}}, 3]},
                ]}}},
                {"$project": {"partner_id": "$user_id",
                              "partner_sn": "$screen_name",
                              "partner_loc": "$location",
                              "shared": {"$setIntersection": ["$tags", "$$mytags"]}}},
            ],
            "as": "match",
        }},
        {"$unwind": "$match"},
        {"$project": {
            "_id": 0,
            "user_a": "$screen_name",
            "user_b": "$match.partner_sn",
            "shared_attr": {"$cond": [
                {"$and": [{"$ne": ["$location", None]},
                          {"$eq": ["$location", "$match.partner_loc"]}]},
                "location", "url_domain"]},
            "day": "$day",
            "shared_hashtags": "$match.shared",
            "support_count": {"$size": "$match.shared"},
        }},
        {"$sort": {"support_count": -1}},
        {"$limit": limit},
    ]
    return list(db.user_day_hashtags.aggregate(pipeline, allowDiskUse=True))

# C-b: Cross-account Promotion Chains

def query_c_b(db, limit=10):
    """A->B edge when B posts a domain A posted within the prior 2 min AND B
    @mentions A. Rank top edges by frequency; report median latency (seconds).
    """
    pipeline = [
        {"$match": {"has_link": True, "created_at": {"$ne": None}}},
        {"$unwind": "$urls"},
        {"$match": {"urls.domain": {"$ne": None}}},
        {"$lookup": {"from": "users", "localField": "user_id",
                     "foreignField": "user_id", "as": "u"}},
        {"$addFields": {"a_sn": {"$toLower": {"$first": "$u.screen_name"}}}},
        {"$project": {"domain": "$urls.domain", "t": "$created_at",
                      "a_id": "$user_id", "a_sn": 1}},
        {"$lookup": {
            "from": "tweets_clean",
            "let": {"dom": "$domain", "ta": "$t", "a": "$a_id", "asn": "$a_sn"},
            "pipeline": [
                {"$match": {"has_link": True, "created_at": {"$ne": None}}},
                {"$unwind": "$urls"},
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$urls.domain", "$$dom"]},
                    {"$ne": ["$user_id", "$$a"]},
                    {"$gt": ["$created_at", "$$ta"]},
                    {"$lte": [{"$dateDiff": {"startDate": "$$ta",
                                             "endDate": "$created_at",
                                             "unit": "second"}}, 120]},
                    {"$in": ["$$asn", {"$map": {"input": "$mentions", "as": "m",
                                                "in": {"$toLower": "$$m"}}}]},
                ]}}},
                {"$project": {"b_id": "$user_id",
                              "latency": {"$dateDiff": {"startDate": "$$ta",
                                                        "endDate": "$created_at",
                                                        "unit": "second"}}}},
            ],
            "as": "b",
        }},
        {"$unwind": "$b"},
        {"$group": {
            "_id": {"src": "$a_id", "dst": "$b.b_id"},
            "events": {"$sum": 1},
            "latencies": {"$push": "$b.latency"},
        }},
        {"$addFields": {
            "median_latency_sec": {
                "$let": {
                    "vars": {"s": {"$sortArray": {"input": "$latencies", "sortBy": 1}},
                             "n": {"$size": "$latencies"}},
                    "in": {"$arrayElemAt": ["$$s", {"$floor": {"$divide": ["$$n", 2]}}]},
                }
            }
        }},
        {"$lookup": {"from": "users", "localField": "_id.src",
                     "foreignField": "user_id", "as": "us"}},
        {"$lookup": {"from": "users", "localField": "_id.dst",
                     "foreignField": "user_id", "as": "ud"}},
        {"$project": {
            "_id": 0,
            "src_user": {"$first": "$us.screen_name"},
            "dst_user": {"$first": "$ud.screen_name"},
            "events": 1,
            "median_latency_sec": 1,
        }},
        {"$sort": {"events": -1, "median_latency_sec": 1}},
        {"$limit": limit},
    ]
    return list(db.tweets_clean.aggregate(pipeline, allowDiskUse=True))


#main

def main():
    ap = argparse.ArgumentParser(description='COMP8210 A1 Section A Part C')
    ap.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    ap.add_argument('--db', default='comp8210')
    args = ap.parse_args()
    db = MongoClient(args.mongo_uri)[args.db]

    print("Running C-a (sock-puppet signals)...")
    print("  (builds helper collection user_day_hashtags, then self-joins)")
    rows = query_c_a(db)
    print_table("C-a: Sock-puppet Signals (shared attr + >=3 common hashtags/day)",
                rows, ["user_a", "user_b", "shared_attr", "day",
                       "support_count", "shared_hashtags"])

    print("\nRunning C-b (promotion chains)...")
    rows = query_c_b(db)
    print_table("C-b: Cross-account Promotion Chains (same domain <=2min + @mention)",
                rows, ["src_user", "dst_user", "events", "median_latency_sec"])


if __name__ == '__main__':
    main()
