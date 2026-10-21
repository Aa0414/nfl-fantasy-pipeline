import time
import os, json, requests, feedparser, datetime as dt
import functions_framework
from google.cloud import storage

BUCKET = os.environ.get("BUCKET", "ba882-nfl-kalshi-raw")   # set at deploy time
HEADERS = {"User-Agent": "Mozilla/5.0 (BA882 class project)"}
FANTASY_POS = {"QB", "RB", "WR", "TE"}
FEEDS = {
    "google": "https://news.google.com/rss/search?q=NFL&hl=en-US&gl=US&ceid=US:en",
    "yahoo": "https://sports.yahoo.com/nfl/rss.xml",
}

def ndjson(rows):
    return "\n".join(json.dumps(r, default=str) for r in rows)

def upload(bucket, path, rows, log):
    bucket.blob(path).upload_from_string(ndjson(rows), content_type="application/x-ndjson")
    log.append(f"uploaded {path} ({len(rows)} rows)")

def current_season():
    return int(requests.get("https://api.sleeper.app/v1/state/nfl", timeout=20).json()["season"])

# ---------- jobs ----------


def job_news(bucket, now, log):
    day, stamp = now.strftime("%Y-%m-%d"), now.strftime("%H-%M")

    # ESPN (bigger pull since we only run daily)
    try:
        arts = requests.get("https://site.api.espn.com/apis/site/v2/sports/football/nfl/news",
                            params={"limit": 200}, headers=HEADERS, timeout=30).json().get("articles", [])
        rows = [{"title": a.get("headline"), "description": a.get("description"),
                 "link": a.get("links", {}).get("web", {}).get("href"),
                 "published": a.get("published"),
                 "tags": [{"type": c.get("type"), "label": c.get("description"),
                           "athlete_id": c.get("athleteId"), "team_id": c.get("teamId")}
                          for c in a.get("categories", [])],
                 "source": "espn", "ingested_at": now.isoformat()} for a in arts]
        if rows: upload(bucket, f"news/espn/dt={day}/{stamp}.ndjson", rows, log)
    except Exception as e:
        log.append(f"espn failed: {e}")

    
    # Google News: a few broad searches (32 per-team searches got blocked)
    QUERIES = {
        "nfl":      "NFL when:1d",
        "injury":   "NFL injury when:1d",
        "fantasy":  "fantasy football when:1d",
        "depth":    "NFL starter OR benched OR promoted when:1d",
    }
    rows, statuses = [], {}
    for label, q in QUERIES.items():
        url = ("https://news.google.com/rss/search?q=" + requests.utils.quote(q)
               + "&hl=en-US&gl=US&ceid=US:en")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            statuses[label] = resp.status_code
            entries = feedparser.parse(resp.content).entries if resp.status_code == 200 else []
        except Exception as e:
            statuses[label] = f"error: {e.__class__.__name__}"
            entries = []
        rows += [{"title": e.get("title"), "link": e.get("link"),
                  "published": e.get("published"), "query": label,
                  "source": "google", "ingested_at": now.isoformat()} for e in entries]
        time.sleep(2)
    log.append(f"google: {len(rows)} rows, statuses {statuses}")
    if rows:
        upload(bucket, f"news/google/dt={day}/{stamp}.ndjson", rows, log)
    else:
        raise RuntimeError(f"google returned 0 rows (statuses {statuses})")

    # Yahoo
    try:
        feed = feedparser.parse(requests.get("https://sports.yahoo.com/nfl/rss.xml",
                                             headers=HEADERS, timeout=20).content)
        rows = [{"title": e.get("title"), "link": e.get("link"),
                 "published": e.get("published"), "source": "yahoo",
                 "ingested_at": now.isoformat()} for e in feed.entries]
        if rows: upload(bucket, f"news/yahoo/dt={day}/{stamp}.ndjson", rows, log)
    except Exception as e:
        log.append(f"yahoo failed: {e}")

def job_players(bucket, now, log):
    players = requests.get("https://api.sleeper.app/v1/players/nfl",
                           params={"active": "true"}, timeout=60).json()
    keep = ["player_id", "full_name", "first_name", "last_name", "position", "team",
            "status", "injury_status", "depth_chart_order", "espn_id", "yahoo_id",
            "gsis_id", "age", "years_exp"]
    rows = [{**{k: p.get(k) for k in keep}, "snapshot_date": now.strftime("%Y-%m-%d")}
            for p in players.values() if p.get("position") in FANTASY_POS]
    upload(bucket, f"players/sleeper/dt={now:%Y-%m-%d}/players.ndjson", rows, log)

def job_stats(bucket, now, log):
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
    job = request.args.get("job", "news")      # default = news (keeps Cloud Scheduler working)
    if job not in JOBS:
        return f"unknown job '{job}', use one of {list(JOBS)}", 400
    bucket = storage.Client().bucket(BUCKET)
    now = dt.datetime.now(dt.timezone.utc)
    log = [f"job={job}"]
    try:
        JOBS[job](bucket, now, log)
    except Exception as e:
        log.append(f"{job} failed: {e}")
        print("\n".join(log))
        return "\n".join(log), 500
    print("\n".join(log))
    return "\n".join(log), 200
