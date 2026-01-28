import logging
import re
import time
from datetime import datetime
from datetime import timedelta
from typing import Any, Hashable

import numpy as np
import pandas as pd
import pendulum
import requests
from airflow.models import Variable
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, task_group
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
_smtp_conn_id = Variable.get("mmix-smtp-conn-id")


def download_article(index: int, url: str, language="ko") -> str:
    logging.info(f"[download][{index}] URL: {url} start")
    try:
        article = Article(url, language=language)
        article.download()
        article.parse()
        return re.sub(r'\n+', '\n', article.text)
    except Exception as e:
        logging.error(f"[download][{index}] URL: {url} error: {e}")
        return ""


def chunk_generator(dataframe, chunk_size):
    for i in range(0, len(dataframe), chunk_size):
        yield dataframe[i:i + chunk_size]


def get_data_interval_end(kwargs: Any, timezone: str = "Asia/Seoul", days: int = 1) -> datetime:
    return get_logical_datetime(kwargs, timezone) - timedelta(days=days)


def get_logical_datetime(kwargs, timezone: str = "Asia/Seoul") -> datetime:
    tz = pendulum.timezone(timezone)
    dag_run = kwargs["dag_run"]
    if dag_run.run_type == DagRunType.MANUAL:
        dt = dag_run.logical_date.astimezone(tz)
    else:
        dt = dag_run.data_interval_end.astimezone(tz)
    return dt.replace(tzinfo=None)


def search_naver_news(subject: str, keyword: str, display=10, page=1, sort="date"):
    time.sleep(0.5)
    start = 1 if page == 1 else page * display + 1
    params = {"query": f"{subject} {keyword}", "display": display, "start": start, "sort": sort}
    try:
        response = requests.get(_naver_openai_search_news_url, headers={"X-Naver-Client-Id": _naver_client_id, "X-Naver-Client-Secret": _naver_client_secret}, params=params)
        response.raise_for_status()
        return response.json().get("items", [])
    except Exception as e:
        logging.error(f"[naver] 검색 실패: {subject} {keyword}, {e}")
        return []


def get_head_line_blocks(date: str, subject_articles: list[dict], per_subject_limit: int = 30, max_blocks: int = 45, max_text_len: int = 2900) -> list[dict]:
    blocks: list[dict] = [{"type": "header", "text": {"type": "plain_text", "text": f"📰 {date} Stock Crawling"}}]
    seen_links = set()
    for group in subject_articles:
        subject = group.get("subject", "NA")
        articles = group.get("article", [])

        def _ts(a: dict) -> float:
            try:
                return datetime.strptime(a.get("published", ""), "%Y-%m-%d %H:%M:%S %z").timestamp()
            except Exception:
                return 0.0

        articles = sorted(articles, key=_ts, reverse=True)
        prefix = f"*[{subject}]*"
        current_text = prefix
        used = 0

        for a in articles:
            if used >= per_subject_limit:
                break
            link = (a.get("link") or "").strip()
            title = (a.get("title") or "").strip()
            if not link or not title:
                continue
            if link in seen_links:
                continue
            seen_links.add(link)

            line = f"\n• <{link}|{title}>"
            if len(current_text) + len(line) > max_text_len:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": current_text}})
                if len(blocks) >= max_blocks:
                    return blocks
                current_text = prefix + " (cont.)" + line
            else:
                current_text += line
            used += 1
        if current_text != prefix:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": current_text}})
            if len(blocks) >= max_blocks:
                return blocks

    return blocks


@dag(dag_id="example_crawling_news",
     default_args={
         "start_date": datetime(2026, 1, 1),
         "retries": 0,
         "retry_delay": timedelta(seconds=10),
     },
     schedule="10 23-13 * * *",
     catchup=False,
     tags=["News", "Crawling", "Slack", "MMIX", "Example"])
def example_crawling_news():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    @task
    def download_stock_keyword(gcp_conn_id: str, spreadsheet_id: str, range_: str = "시트1!A1:Z1000") -> list[dict]:
        hook = GSheetsHook(gcp_conn_id=gcp_conn_id)
        values = hook.get_values(spreadsheet_id, range_)
        logging.info(f"Downloaded values from Google Sheet: {spreadsheet_id}, Range: {range_}\n{values}")
        dataframe = pd.DataFrame(values[1:], columns=values[0]).rename(columns={"주제": "subject", "키워드": "keywords"})
        dataframe['keywords'] = dataframe['keywords'].apply(lambda x: [item.strip() for item in x.split(',')])
        return dataframe.to_dict("records")

    @task_group(group_id="crawling_naver_news_group")
    def crawling_naver_news_group() -> None:
        @task(task_id="crawling_naver_news")
        def crawling_naver_news(indices: int = 5, **kwargs: Any) -> list[dict[str, Any]]:
            logging.info("Starting Naver News Crawling...")
            dataframe_keyword = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='download_stock_keyword'))
            dataframe_keyword_flat = dataframe_keyword.explode("keywords").reset_index(drop=True).rename(columns={'keywords': 'keyword'})
            dataframe_keyword_flat["feeds"] = dataframe_keyword_flat.apply(lambda x: search_naver_news(x["subject"], x["keyword"], display=99), axis=1)
            dataframe_feed_flat = dataframe_keyword_flat[["subject", "keyword", "feeds"]].explode("feeds").reset_index(drop=True).rename(columns={'feeds': 'feed'})
            dataframe_feed_info = pd.concat([dataframe_feed_flat.drop(['feed'], axis=1), pd.json_normalize(dataframe_feed_flat['feed'])], axis=1).fillna("")
            dataframe_feed_group = np.array_split(dataframe_feed_info, indices)
            return [{"index": index, "dicts": dataframe.to_dict(orient='records')} for index, dataframe in enumerate(dataframe_feed_group)]

        @task(task_id="download_naver_news")
        def download_naver_news(index: int, dicts: list[dict[str, Any]], **kwargs: Any) -> None:
            since_hours = (kwargs["logical_date"] - timedelta(hours=1)).in_timezone("Asia/Seoul")
            dataframe_rename = pd.DataFrame(dicts).rename(columns={"originallink": "original_link", "pubDate": "published", "description": "summary"})
            published_dt = pd.to_datetime(dataframe_rename["published"], format="%a, %d %b %Y %H:%M:%S %z", errors="coerce")
            dataframe_rename["published_dt"] = published_dt.dt.tz_convert("Asia/Seoul")
            dataframe_since_1h = dataframe_rename[dataframe_rename["published_dt"] >= since_hours].copy()
            dataframe_since_1h["published"] = dataframe_since_1h["published_dt"].dt.strftime("%Y-%m-%d %H:%M:%S %z")
            dataframe_selection = dataframe_since_1h[["subject", "published", "keyword", "title", "summary", "original_link", "link"]]
            dataframe_selection = dataframe_selection.assign(title=dataframe_selection["title"].apply(strip_html))
            dataframe_selection["description"] = dataframe_selection['original_link'].apply(lambda x: download_article(index, x))
            kwargs['ti'].xcom_push(key=f'download_naver_news_{index}', value=dataframe_selection.to_dict(orient='records'))

        download_naver_news.expand_kwargs(crawling_naver_news())

    @task(task_id="merge_naver_news")
    def merge_naver_news(indices=5, **kwargs: Any) -> list[dict[Hashable, Any]]:
        logging.info("Merging DataFrames from Naver News")
        return [item for sublist in [kwargs['ti'].xcom_pull(task_ids='crawling_naver_news_group.download_naver_news', key=f'download_naver_news_{index}') for index in range(indices)] for item in sublist]

    @task(task_id="load_to_mysql")
    def load_to_mysql(mysql_conn_id: str, chuck: int = 10, **kwargs: Any) -> None:
        logging.info("Loading data into MySQL")
        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        connection = None
        cursor = None
        dataframe = pd.DataFrame(kwargs['ti'].xcom_pull(task_ids='merge_naver_news'))
        dataframe_filtered = dataframe[dataframe["title"].notna() & dataframe["original_link"].notna() & (dataframe["title"].str.strip() != "") & (dataframe["original_link"].str.strip() != "")]
        dataframe_filtered["published"] = pd.to_datetime(dataframe_filtered["published"], errors="coerce").dt.tz_localize(None)

        if len(dataframe) == 0 or len(dataframe_filtered) == 0:
            logging.warning("No data to load into MySQL")
            return
        logging.info(f"DataFrame to be loaded into : {dataframe_filtered.info()}")
        total = int(np.ceil(len(dataframe_filtered) / chuck))
        dataframe_group = chunk_generator(dataframe_filtered, chuck)
        try:
            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.execute("BEGIN;")
            for index, df in enumerate(dataframe_group):
                logging.info(f"Loading DataFrame {index + 1}/{total} into MySQL... {df.info()}")
                values_list = []
                for _, row in df.iterrows():
                    values = [row['subject'], row['keyword'], row['published'], row['title'], row['summary'], row['description'], row['original_link'], row['link']]
                    logging.info(f"load_to_aurora type: published:{row['published']} keyword:{row['keyword']} title:{len(row['title'])} link:{row['link']} original_link:{row['original_link']}")
                    values_list.append(tuple(values))
                insert_query = "INSERT INTO news (subject, keyword, published, title, summary, description, original_link, link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.executemany(insert_query, values_list)
            connection.commit()
        except Exception as e:
            logging.error(f"Error loading data into Aurora: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            logging.info("Connection and cursor closed")

        logging.info("Data loaded into Aurora successfully.")

    @task
    def head_line_send_to_slack(rows: list[dict], slack_trend_conn_id, slack_trend_channel_id, **kwargs) -> SlackResponse:
        date = get_logical_datetime(kwargs).strftime('%Y-%m-%d %H')
        dataframe = pd.DataFrame(rows)
        dataframe_filtered = dataframe[dataframe["title"].notna() & dataframe["original_link"].notna() & (dataframe["title"].str.strip() != "") & (dataframe["original_link"].str.strip() != "")]
        subject_article = [{"subject": subject_, "article": dataframe_.to_dict("records")} for subject_, dataframe_ in dataframe_filtered.groupby('subject') if subject_ != "NA"]
        logging.info(f"Subject articles for Slack: {subject_article}")
        response = SlackHook(slack_conn_id=slack_trend_conn_id).client.chat_postMessage(channel=slack_trend_channel_id, blocks=get_head_line_blocks(date, subject_article), unfurl_links=False, unfurl_media=False)
        logging.info(f"Slack message sent successfully: {response['ok']}")
        return response["ok"]

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

    download_news_keyword_task = download_stock_keyword(_gcp_conn_id, _news_keyword_google_sheet_id, "시트1!A1:Z1000")
    crawling_naver_news_group_task = crawling_naver_news_group()
    merge_news_task = merge_naver_news()
    head_line_send_to_slack_task = head_line_send_to_slack(merge_news_task, _slack_conn_id, _slack_channel_id)
    send_to_mail_task = send_to_mail(merge_news_task, download_news_keyword_task, _smtp_conn_id)

    (start_task >> download_news_keyword_task >> crawling_naver_news_group_task >> merge_news_task >> load_to_mysql(_mysql_conn_id) >> head_line_send_to_slack_task >> send_to_mail_task >> end_task)


example_crawling_news()
