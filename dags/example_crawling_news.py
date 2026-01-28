import html
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import pendulum
import requests
from airflow.models import Variable
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from airflow.utils.types import DagRunType

from newspaper import Article
from slack_sdk.web import SlackResponse

from common.mmix_utils import strip_html, render_news_email_content

_slack_conn_id = Variable.get("mmix-slack-conn-id")
_slack_channel_id = Variable.get("mmix-slack-channel-id")
_naver_client_id = Variable.get("mmix-naver-client-id")
_naver_client_secret = Variable.get("mmix-naver-client-secret")
_naver_openai_search_news_url = Variable.get("mmix-naver-openai-search-news-url")
_news_keyword_google_sheet_id = Variable.get("mmix-news-keyword-google-sheet-id")
_gcp_conn_id = Variable.get("mmix-gcp-conn-id")
_mysql_conn_id = Variable.get("mmix-mysql-primary-external-conn-id")
_smtp_conn_id = Variable.get("mmix-smtp-conn-id", default_var=None)


def get_logical_datetime(kwargs: dict, timezone: str = "Asia/Seoul") -> datetime:
    tz = pendulum.timezone(timezone)
    dag_run = kwargs["dag_run"]
    if dag_run.run_type == DagRunType.MANUAL:
        dt = dag_run.logical_date.astimezone(tz)
    else:
        dt = dag_run.data_interval_end.astimezone(tz)
    return dt.replace(tzinfo=None)


def parse_naver_pubdate(pub_date: str) -> datetime | None:
    # 예: "Mon, 27 Jan 2026 13:05:00 +0900"
    try:
        dt = pd.to_datetime(pub_date, format="%a, %d %b %Y %H:%M:%S %z", errors="raise")
        return dt.tz_convert("Asia/Seoul").to_pydatetime()
    except Exception:
        return None


def download_article_text(url: str, language: str = "ko", max_chars: int = 20000) -> str:
    try:
        article = Article(url, language=language)
        article.download()
        article.parse()
        text = re.sub(r"\n+", "\n", article.text or "")
        if len(text) > max_chars:
            return text[:max_chars]
        return text
    except Exception as e:
        logging.warning(f"[article] download failed url={url} err={e}")
        return ""


def naver_search_news(session: requests.Session, subject: str, keyword: str, display: int = 99, page: int = 1, sort: str = "date") -> list[dict]:
    start = 1 if page == 1 else page * display + 1
    params = {"query": f"{subject} {keyword}", "display": display, "start": start, "sort": sort}
    headers = {"X-Naver-Client-Id": _naver_client_id, "X-Naver-Client-Secret": _naver_client_secret}
    last_err = None
    for i in range(3):
        try:
            r = session.get(_naver_openai_search_news_url, headers=headers, params=params, timeout=(3, 12))
            r.raise_for_status()
            return r.json().get("items", []) or []
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    logging.error(f"[naver] search failed subject={subject} keyword={keyword} err={last_err}")
    return []


def get_head_line_blocks(date_label: str, subject_articles: list[dict], per_subject_limit: int = 30, total_limit: int = 120) -> list[dict]:
    blocks = [{"type": "header", "text": {"type": "plain_text", "text": f"📰 {date_label} Stock Crawling"}}]
    seen_links = set()
    total_added = 0

    for group in subject_articles:
        subject = group["subject"]
        articles = group["article"]
        articles = sorted(articles, key=lambda x: x.get("published_dt", ""), reverse=True)
        topic_block = {"type": "section", "text": {"type": "mrkdwn", "text": f"*[{subject}]*"}}
        subject_count = 0
        for a in articles:
            link = a.get("link") or ""
            title = a.get("title") or ""
            if not link or not title:
                continue
            if link in seen_links:
                continue

            seen_links.add(link)
            topic_block["text"]["text"] += f"\n• <{link}|{title}>"
            subject_count += 1
            total_added += 1

            if subject_count >= per_subject_limit or total_added >= total_limit:
                break

        blocks.append(topic_block)
        if total_added >= total_limit:
            break

    return blocks


def chunk_list(items: list[dict], chunk_size: int) -> list[list[dict]]:
    return [items[i: i + chunk_size] for i in range(0, len(items), chunk_size)]


@dag(
    dag_id="example_crawling_news",
    default_args={
        "start_date": datetime(2026, 1, 1),
        "retries": 0,
        "retry_delay": timedelta(seconds=10),
    },
    schedule="0 * * * *",
    catchup=False,
    tags=["News", "Crawling", "Slack", "MMIX", "Optimized"],
)
def example_crawling_news():
    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    @task
    def download_stock_keyword(gcp_conn_id: str, spreadsheet_id: str, range_: str = "시트1!A1:Z1000") -> list[dict]:
        hook = GSheetsHook(gcp_conn_id=gcp_conn_id)
        values = hook.get_values(spreadsheet_id, range_)
        if not values or len(values) < 2:
            return []

        df = (
            pd.DataFrame(values[1:], columns=values[0])
            .rename(columns={"주제": "subject", "키워드": "keywords"})
            .fillna("")
        )
        df = df[(df["subject"].str.strip() != "") & (df["keywords"].str.strip() != "")]
        df["keywords"] = df["keywords"].apply(lambda x: [item.strip() for item in x.split(",") if item.strip()])
        return df.to_dict("records")

    @task
    def build_search_jobs(keyword_records: list[dict]) -> list[dict]:
        if not keyword_records:
            return []
        df = pd.DataFrame(keyword_records)
        df = df.explode("keywords").reset_index(drop=True).rename(columns={"keywords": "keyword"})
        df = df[(df["subject"].str.strip() != "") & (df["keyword"].str.strip() != "")]
        return df[["subject", "keyword"]].to_dict("records")

    @task
    def search_naver_for_job(job: dict, **kwargs: Any) -> list[dict]:
        subject = job["subject"]
        keyword = job["keyword"]

        logical_dt = kwargs["logical_date"].in_timezone("Asia/Seoul")
        since_dt = logical_dt - timedelta(hours=1)

        with requests.Session() as session:
            items = naver_search_news(session, subject, keyword, display=99)

        results: list[dict] = []
        for it in items:
            pub = it.get("pubDate", "")
            pub_dt = parse_naver_pubdate(pub)
            if not pub_dt:
                continue
            if pub_dt < since_dt:
                continue

            title = strip_html(it.get("title", ""))
            summary = strip_html(it.get("description", ""))

            originallink = it.get("originallink", "") or ""
            link = it.get("link", "") or ""

            if not title.strip() or not originallink.strip():
                continue

            results.append({"subject": subject, "keyword": keyword, "published": pub_dt.strftime("%Y-%m-%d %H:%M:%S"), "published_dt": pub_dt.isoformat(), "title": title, "summary": summary, "original_link": originallink, "link": link})
        return results

    @task
    def flatten_items(items_per_job: list[list[dict]]) -> list[dict]:
        if not items_per_job:
            return []
        merged: list[dict] = []
        seen = set()
        for sub in items_per_job:
            for it in (sub or []):
                k = it.get("original_link", "")
                if not k or k in seen:
                    continue
                seen.add(k)
                merged.append(it)
        return merged

    @task
    def chunk_items_task(items: list[dict], chunk_size: int = 10) -> list[list[dict]]:
        if not items:
            return []
        return chunk_list(items, chunk_size)

    @task
    def download_and_upsert_chunk(chunk: list[dict], mysql_conn_id: str) -> int:
        if not chunk:
            return 0

        rows = []
        for it in chunk:
            desc = download_article_text(it["original_link"])
            rows.append((it["subject"], it["keyword"], it["published"], it["title"], it["summary"], desc, it["original_link"], it["link"]))

        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            upsert_sql = """
                         INSERT INTO news
                             (subject, keyword, published, title, summary, description, original_link, link)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                         ON DUPLICATE KEY UPDATE published=VALUES(published),
                                                 title=VALUES(title),
                                                 summary=VALUES(summary),
                                                 description=VALUES(description),
                                                 link=VALUES(link),
                                                 keyword=VALUES(keyword),
                                                 subject=VALUES(subject) \
                         """
            cursor.executemany(upsert_sql, rows)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            conn.rollback()
            logging.error(f"[mysql] upsert failed err={e}")
            raise
        finally:
            cursor.close()
            conn.close()

    @task
    def query_recent_news(mysql_conn_id: str, **kwargs: Any) -> list[dict]:
        logical_dt = kwargs["logical_date"].in_timezone("Asia/Seoul")
        since_dt = (logical_dt - timedelta(hours=1)).replace(tzinfo=None)
        until_dt = logical_dt.replace(tzinfo=None)

        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        sql = """
              SELECT subject,
                     keyword,
                     published,
                     title,
                     summary,
                     description,
                     original_link,
                     link
              FROM news
              WHERE published >= %s
                AND published < %s
              ORDER BY published DESC \
              """
        df = hook.get_pandas_df(sql, parameters=(since_dt, until_dt))
        if df is None or df.empty:
            return []

        df["published_dt"] = pd.to_datetime(df["published"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        return df.to_dict("records")

    @task
    def send_headline_to_slack(slack_conn_id: str, channel_id: str, rows: list[dict], **kwargs: Any) -> SlackResponse | None:
        if not rows:
            logging.info("[slack] no recent news - skip")
            return None

        date_label = get_logical_datetime(kwargs).strftime("%Y-%m-%d %H")
        df = pd.DataFrame(rows)

        subject_article = []
        for subject, sdf in df.groupby("subject"):
            articles = sdf.to_dict("records")
            for a in articles:
                a["published_dt"] = a.get("published_dt", "")
            subject_article.append({"subject": subject, "article": articles})

        blocks = get_head_line_blocks(date_label, subject_article, per_subject_limit=30, total_limit=120)
        resp = SlackHook(slack_conn_id=slack_conn_id).client.chat_postMessage(channel=channel_id, blocks=blocks, unfurl_links=False, unfurl_media=False)
        logging.info(f"[slack] ok={resp.get('ok')}")
        return resp

    @task
    def send_to_mail(rows: list[dict], keyword_records_: list[dict], smtp_conn_id: str | None, **kwargs: Any) -> bool:
        if not smtp_conn_id:
            logging.info("[mail] smtp conn id not set - skip")
            return True

        generated_at = kwargs["logical_date"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M")
        subject = f"Stock News Report - {generated_at}"

        if not rows:
            logging.info("[mail] no recent news - skip")
            return True

        html_content = render_news_email_content(rows, generated_at=generated_at, keyword_records=keyword_records_)
        with SmtpHook(smtp_conn_id=smtp_conn_id) as hook:
            hook.send_email_smtp(to=["csj4032@gmail.com"], subject=subject, html_content=html_content)
        return True

    keyword_records = download_stock_keyword(_gcp_conn_id, _news_keyword_google_sheet_id, "시트1!A1:Z1000")
    search_jobs = build_search_jobs(keyword_records)
    items_per_job = search_naver_for_job.expand(job=search_jobs)
    items = flatten_items(items_per_job)
    chunks = chunk_items_task(items, chunk_size=10)
    _ = download_and_upsert_chunk.expand(chunk=chunks, mysql_conn_id=[_mysql_conn_id] * 1000000)
    recent_rows = query_recent_news(_mysql_conn_id)
    slack_task = send_headline_to_slack(_slack_conn_id, _slack_channel_id, recent_rows)
    mail_task = send_to_mail(recent_rows, keyword_records, _smtp_conn_id)
    start_task >> keyword_records >> search_jobs >> items_per_job >> items >> chunks
    chunks >> _ >> recent_rows >> [slack_task, mail_task] >> end_task


example_crawling_news()
