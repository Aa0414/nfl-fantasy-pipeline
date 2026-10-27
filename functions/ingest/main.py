import os, json, time, requests, feedparser, datetime as dt
import functions_framework
from google.cloud import storage

BUCKET = os.environ.get("BUCKET", "ba882-nfl-kalshi-raw")
HEADERS = {"User-Agent": "Mozilla/5.0 (BA882 class project)"}
FANTASY_POS = {"QB", "RB", "WR", "TE"}
GOOGLE_QUERIES = {
    "nfl":     "NFL",
    "injury":  "NFL injury",
    "fantasy": "fantasy football",
    "depth":   "NFL starter OR benched OR promoted",
}

def ndjson(rows):
    return "\n".join(json.dumps(r, default=str) for r in rows)

def upload(bucket, path, rows, log):
    bucket.blob(path).upload_from_string(ndjson(rows), content_type="application/x-ndjson")
    log.append(f"uploaded {path} ({len(rows)} rows)")

def current_season():
    return int(requests.get("https://api.sleeper.app/v1/state/nfl", timeout=20).json()["season"])

# ---------- jobs ----------
def job_news(bucket, now, log, args):
    day, stamp = now.strftime("%Y-%m-%d"), now.strftime("%H-%M")
    window = args.get("window", "1d")          # use ?window=7d once to backfill Google
    errors = []

    # ESPN
    try:
        arts = requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/news",
                            params={"limit": 50}, headers=HEADERS, timeout=30).json().get("articles", [])
        rows = [{"title": a.get("headline"), "description": a.get("description"),
                 "link": a.get("links", {}).get("web", {}).get("href"),
                 "published": a.get("published"),
                 "tags": [{"type": c.get("type"), "label": c.get("description"),
                           "athlete_id": c.get("athleteId"), "team_id": c.get("teamId")}
                          for c in a.get("categories", [])],
                 "source": "espn", "ingested_at": now.isoformat()} for a in arts]
        if rows:
            upload(bucket, f"news/espn/dt={day}/{stamp}.ndjson", rows, log)
        else:
            errors.append("espn returned 0 rows")
    except Exception as e:
        errors.append(f"espn failed: {e}")

    # Google News: 4 broad searches, retry each up to 3 times
    rows, statuses = [], {}
    for label, q in GOOGLE_QUERIES.items():
        url = ("https://news.google.com/rss/search?q=" + requests.utils.quote(f"{q} when:{window}")
               + "&hl=en-US&gl=US&ceid=US:en")
        entries = []
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=10)
                statuses[label] = resp.status_code
                if resp.status_code == 200:
                    entries = feedparser.parse(resp.content).entries
                    if entries:
                        break
            except Exception as e:
                statuses[label] = f"error: {e.__class__.__name__}"
            time.sleep(3 * (attempt + 1))
        rows += [{"title": e.get("title"), "link": e.get("link"),
                  "published": e.get("published"), "query": label,
                  "source": "google", "ingested_at": now.isoformat()} for e in entries]
        time.sleep(2)
    log.append(f"google: {len(rows)} rows, window {window}, statuses {statuses}")
    if rows:
        upload(bucket, f"news/google/dt={day}/{stamp}.ndjson", rows, log)
    else:
        errors.append(f"google returned 0 rows (statuses {statuses})")

    # Yahoo
    try:
        feed = feedparser.parse(requests.get("https://sports.yahoo.com/nfl/rss.xml",
                                             headers=HEADERS, timeout=20).content)
        rows = [{"title": e.get("title"), "link": e.get("link"),
                 "published": e.get("published"), "source": "yahoo",
                 "ingested_at": now.isoformat()} for e in feed.entries]
        if rows:
            upload(bucket, f"news/yahoo/dt={day}/{stamp}.ndjson", rows, log)
        else:
            errors.append("yahoo returned 0 rows")
    except Exception as e:
        errors.append(f"yahoo failed: {e}")

    # Fail only AFTER every source has run
    if errors:
        raise RuntimeError("; ".join(errors))

def job_players(bucket, now, log, args):
    players = requests.get("https://api.sleeper.app/v1/players/nfl",
                           params={"active": "true"}, timeout=60).json()
    keep = ["player_id", "full_name", "first_name", "last_name", "position", "team",
            "status", "injury_status", "depth_chart_order", "espn_id", "yahoo_id",
            "gsis_id", "age", "years_exp"]
    rows = [{**{k: p.get(k) for k in keep}, "snapshot_date": now.strftime("%Y-%m-%d")}
            for p in players.values() if p.get("position") in FANTASY_POS]
    upload(bucket, f"players/sleeper/dt={now:%Y-%m-%d}/players.ndjson", rows, log)

def job_stats(bucket, now, log, args):
    import nflreadpy as nfl
    season = current_season()
    stats = nfl.load_player_stats([season])
    sched = nfl.load_schedules([season])
    upload(bucket, f"stats/player_week/season={season}/pulled={now:%Y-%m-%d}.ndjson",
           stats.to_dicts(), log)
    upload(bucket, f"stats/schedule/season={season}/pulled={now:%Y-%m-%d}.ndjson",
           sched.to_dicts(), log)

JOBS = {"news": job_news, "players": job_players, "stats": job_stats}

@functions_framework.http
def run(request):
    job = request.args.get("job", "news")
    if job not in JOBS:
        return f"unknown job '{job}', use one of {list(JOBS)}", 400
    bucket = storage.Client().bucket(BUCKET)
    now = dt.datetime.now(dt.timezone.utc)
    log = [f"job={job}"]
    try:
        JOBS[job](bucket, now, log, request.args)
    except Exception as e:
        log.append(f"{job} failed: {e}")
        print("\n".join(log))
        return "\n".join(log), 500
    print("\n".join(log))
    return "\n".join(log), 200
