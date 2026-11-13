import argparse
import json
import re
from datetime import datetime
from urllib.parse import urlparse
from pymongo import MongoClient, ASCENDING, UpdateOne

# Helpers

# regex patterns I reuse for cleaning: one matches URLs, one matches @mentions
URL_RE = re.compile(r'https?://\S+')
MENTION_RE = re.compile(r'@\w+')


def parse_dt(s):
    # turns a date string into a real datetime object.
    # the 10k file and the 50-tweet sample store dates differently, so I try both formats: the ISO format first, then the classic Twitter format.
    # returns None if it can't parse 
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except ValueError:
        pass
    try:
        return datetime.strptime(s, '%a %b %d %H:%M:%S %z %Y')
    except ValueError:
        return None


def domain_of(url):
    # pulls just the domain out of a URL.
    # I strip the "www." so the same site always gives the same domain.
    if not url:
        return None
    try:
        net = urlparse(url).netloc.lower()
        return net[4:] if net.startswith('www.') else net
    except Exception:
        return None


def clean_text(text):
    # makes the clean_text field: removes URLs and @mentions from the tweet text and squashes extra spaces. 
    # this is what Part C uses to find duplicate tweets.
    if not text:
        return ''
    t = URL_RE.sub('', text)
    t = MENTION_RE.sub('', t)
    return re.sub(r'\s+', ' ', t).strip()


# MAP phase: takes one raw tweet and returns a cleaned tweet + its user.

def transform_record(doc):
    # first it checks the format
    # the sample is flat, so the same code works on both datasets.
    is_gnip = doc.get('objectType') == 'activity' or 'actor' in doc

    if is_gnip:
    
        obj = doc.get('object', {}) or {}
        actor = doc.get('actor', {}) or {}
        ent = doc.get('twitter_entities', {}) or {}

        id_str = str(obj.get('id') or doc.get('id'))
        # I take the date from object.postedTime, NOT the top-level postedTime
        created = parse_dt(obj.get('postedTime'))
        text = doc.get('text') or obj.get('summary') or ''
        # pull out the arrays the assignment asks for: hashtags, mentions, and urls 
        hashtags = [h.get('text', '').lower()
                    for h in ent.get('hashtags', []) if h.get('text')]
        mentions = [m.get('screen_name')
                    for m in ent.get('user_mentions', []) if m.get('screen_name')]
        urls = [{'expanded_url': u.get('expanded_url') or u.get('url'),
                 'domain': domain_of(u.get('expanded_url') or u.get('url'))}
                for u in ent.get('urls', [])]

        lang = doc.get('twitter_lang') or \
            (doc.get('gnip', {}).get('language', {}) or {}).get('value')

        loc = actor.get('location', {})
        location = loc.get('displayName') if isinstance(loc, dict) else loc
        links = actor.get('links', []) or []
        url_domain = domain_of(links[0].get('href')) if links else None
        # build the user record from the "actor" part of the Gnip data
        user = {
            'user_id': str(actor.get('id')),
            'screen_name': actor.get('preferredUsername'),
            'name': actor.get('displayName'),
            'location': location,
            'url_domain': url_domain,
        }
    else:
        # the flat 50-tweet sample format. same idea, but the fields sit in different places 
        u = doc.get('user', {}) or {}
        ent = doc.get('entities', {}) or {}

        id_str = str(doc.get('id_str'))
        created = parse_dt(doc.get('created_at'))
        text = doc.get('text', '')

        hashtags = [h.get('text', '').lower()
                    for h in ent.get('hashtags', []) if h.get('text')]
        mentions = [m.get('screen_name')
                    for m in ent.get('user_mentions', []) if m.get('screen_name')]
        urls = [{'expanded_url': (x.get('unwound') or {}).get('url') or x.get('url'),
                 'domain': domain_of((x.get('unwound') or {}).get('url') or x.get('url'))}
                for x in ent.get('urls', [])]

        lang = doc.get('lang')
        user = {
            'user_id': str(u.get('id')),
            'screen_name': u.get('screen_name'),
            'name': u.get('name'),
            'location': u.get('location'),
            'url_domain': domain_of(u.get('url')),
        }

    ct = clean_text(text)
    tweet = {
        'id_str': id_str,
        'created_at': created,
        'text': text,
        'clean_text': ct,
        'hashtags': hashtags,
        'mentions': mentions,
        'urls': urls,
        'user_id': user['user_id'],
        # derived fields
        'hour_of_day': created.hour if created else None,
        'weekday': created.strftime('%A') if created else None,
        'has_link': len(urls) > 0,
        'lang_guess': lang or ('en' if ct.isascii() else 'und'),
    }
    return tweet, user

# REDUCE STEP: removes duplicate tweets.

def dedupe_latest(tweets):
    #groups by id_str and keeps the one with the newest created_at. 
    best = {}
    for t in tweets:
        k = t['id_str']
        cur = best.get(k)
        if cur is None:
            best[k] = t
        else:
            a, b = t['created_at'], cur['created_at']
            if a and b and a > b:
                best[k] = t
            elif b is None and a is not None:
                best[k] = t
    return list(best.values())

# LOAD phase: writes everything into MongoDB and builds the indexes.

def load(db, raw_docs, tweets_clean, users):
    # tweets_raw: an untouched copy of the original data, kept for reference
    db.tweets_raw.drop()
    if raw_docs:
        db.tweets_raw.insert_many(raw_docs, ordered=False)

    # tweets_clean
    db.tweets_clean.drop()
    if tweets_clean:
        db.tweets_clean.insert_many(tweets_clean, ordered=False)

    # users: split into its own collection. upsert means re-running updates.
    db.users.drop()
    if users:
        ops = [UpdateOne({'user_id': u['user_id']}, {'$set': u}, upsert=True)
               for u in users.values()]
        db.users.bulk_write(ops, ordered=False)

    # Indexes 
    db.tweets_clean.create_index([('created_at', ASCENDING)])
    db.tweets_clean.create_index([('hashtags', ASCENDING), ('created_at', ASCENDING)])
    db.tweets_clean.create_index([('user_id', ASCENDING), ('created_at', ASCENDING)])
    db.tweets_clean.create_index([('urls.domain', ASCENDING), ('created_at', ASCENDING)])
    db.users.create_index([('user_id', ASCENDING)], unique=True)
    db.users.create_index([('screen_name', ASCENDING)])


# main: reads the file, runs map -> reduce -> load, and prints a summary

def main():
    ap = argparse.ArgumentParser(description='COMP8210 A1 Section A Part A - curation pipeline')
    ap.add_argument('--input', required=True, help='Path to raw tweets JSON')
    ap.add_argument('--mongo-uri', default='mongodb://localhost:27017')
    ap.add_argument('--db', default='comp8210')
    args = ap.parse_args()

    print(f'Reading {args.input} ...')
    with open(args.input, 'r', encoding='utf-8') as f:
        raw = json.load(f)
    if isinstance(raw, dict):          # tolerate a single object
        raw = [raw]
    print(f'  {len(raw)} raw records')

    # MAP: transform every raw record into a clean tweet + user.
    tweets, users = [], {}
    for doc in raw:
        t, u = transform_record(doc)
        tweets.append(t)
        if u.get('user_id'):
            users[u['user_id']] = u
    print(f'  mapped {len(tweets)} tweets, {len(users)} unique users')

    # REDUCE: drop duplicate tweets, keep the latest version of each
    before = len(tweets)
    tweets = dedupe_latest(tweets)
    print(f'  deduped: {before} -> {len(tweets)} (removed {before - len(tweets)})')

    # LOAD: connect to MongoDB and write the collections + indexes
    print(f'Connecting to {args.mongo_uri}, db={args.db} ...')
    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    load(db, raw, tweets, users)

    # print the final counts and indexes so I can confirm it worked
    print('Done. Collection counts:')
    print(f'  tweets_raw   : {db.tweets_raw.count_documents({})}')
    print(f'  tweets_clean : {db.tweets_clean.count_documents({})}')
    print(f'  users        : {db.users.count_documents({})}')
    print('Indexes on tweets_clean:', [i["name"] for i in db.tweets_clean.list_indexes()])
    print('Indexes on users       :', [i["name"] for i in db.users.list_indexes()])


if __name__ == '__main__':
    main()
