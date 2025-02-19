import argparse
from datetime import datetime

from collector.google_trend_collector.google_trend_collector import GoogleTrendCollector
from utils.converter import adjust_job_id

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", help="language", default=None)
    args = parser.parse_args()

    if args.lang == "en":
        jobid = adjust_job_id(datetime.now().strftime("%Y%m%d%H"), 14)
        google_trend_collector = GoogleTrendCollector(lang=args.lang, jobid=jobid)
    else:
        google_trend_collector = GoogleTrendCollector(args.lang)
    google_trend_collector.run()