import html
import json
from collections import defaultdict
from datetime import datetime

from airflow.models import Variable
from airflow.sdk import BaseHook
from bs4 import BeautifulSoup


def build_mysql_conn_json(conn_id_variable_name: str) -> str:
    conn_id = Variable.get(conn_id_variable_name)
    conn = BaseHook.get_connection(conn_id)
    payload = {"host": conn.host, "port": conn.port, "user": conn.login, "password": conn.password, "database": conn.schema, "driver": conn.extra_dejson.get("driver", "com.mysql.cj.jdbc.Driver")}
    return json.dumps(payload, separators=(",", ":"))


def build_postgresql_conn_json(conn_id_variable_name: str) -> str:
    conn_id = Variable.get(conn_id_variable_name)
    conn = BaseHook.get_connection(conn_id)
    payload = {"host": conn.host, "port": conn.port, "user": conn.login, "password": conn.password, "database": conn.schema, "driver": conn.extra_dejson.get("driver", "org.postgresql.Driver")}
    return json.dumps(payload, separators=(",", ":"))


def strip_html(raw_html: str) -> str:
    if not isinstance(raw_html, str) or not raw_html.strip():
        return ""
    return html.unescape(BeautifulSoup(raw_html, "html.parser").get_text(separator=" ", strip=True))


def render_news_email_content(rows: list[dict[str, any]], title: str = "📰 Stock News Digest", generated_at: str | None = None, per_subject_limit: int = 25, keyword_records: list[dict[str, any]] | None = None) -> str:
    generated_at = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M")

    def esc(x: str) -> str:
        return html.escape(x or "", quote=True)

    keyword_sections: list[str] = []
    for r in (keyword_records or []):
        subject = (r.get("subject") or "NA").strip() or "NA"
        kws = r.get("keywords") or []
        kws = [k.strip() for k in kws if isinstance(k, str) and k.strip()]
        if not kws:
            continue
        keyword_sections.append(
            f"""
            <div style="margin-top:10px;">
              <div style="font-size:13px;font-weight:900;color:#111827;">[{esc(subject)}]</div>
              <div style="margin-top:6px;font-size:12px;color:#374151;line-height:1.6;">
                {' '.join([f'<span style="display:inline-block;margin:0 6px 6px 0;padding:4px 8px;border-radius:999px;background:#f3f4f6;border:1px solid #e5e7eb;">#{esc(k)}</span>' for k in kws])}
              </div>
            </div>
            """
        )

    keyword_block_html = ""
    if keyword_sections:
        keyword_block_html = f"""
        <div style="margin-top:12px;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:14px 18px;">
          <div style="font-size:13px;color:#6b7280;">오늘 수집 키워드</div>
          {''.join(keyword_sections)}
        </div>
        """

    seen = set()
    cleaned: list[dict[str, any]] = []
    for r in rows or []:
        link = (r.get("link") or "").strip()
        original = (r.get("original_link") or "").strip()
        uniq_key = original or link
        if not uniq_key or uniq_key in seen:
            continue
        seen.add(uniq_key)
        cleaned.append({
            "subject": (r.get("subject") or "NA").strip() or "NA",
            "keyword": (r.get("keyword") or "").strip(),
            "title": strip_html_light(r.get("title") or ""),
            "published": (r.get("published") or "").strip(),
            "published_dt": parse_published_kst(r.get("published") or ""),
            "link": link,
            "original_link": original,
        })

    groups: dict[str, list[dict[str, any]]] = defaultdict(list)
    for r in cleaned:
        groups[r["subject"]].append(r)

    for s in list(groups.keys()):
        groups[s].sort(key=lambda x: x["published_dt"], reverse=True)
        groups[s] = groups[s][:per_subject_limit]

    total_count = sum(len(v) for v in groups.values())

    subject_sections: list[str] = []
    for subject, items in groups.items():
        if subject == "NA":
            continue

        lis: list[str] = []
        for it in items:
            time_txt = (
                it["published_dt"].strftime("%Y-%m-%d %H:%M")
                if it["published_dt"].year > 1971
                else it["published"]
            )
            primary_url = it["original_link"] or it["link"]
            kw = it["keyword"]
            lis.append(f"""
              <li style="margin:0 0 10px 0; line-height:1.55;">
                <a href="{esc(primary_url)}" style="color:#111827;text-decoration:none;font-weight:700;">
                  {esc(it["title"])}
                </a>
                <div style="margin-top:2px;font-size:12px;color:#6b7280;">
                  <span style="font-weight:600;">{esc(time_txt)}</span>
                  {f'<span style="margin-left:8px;">#{esc(kw)}</span>' if kw else ''}
                </div>
              </li>
            """)

        subject_sections.append(f"""
          <div style="margin-top:18px;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:14px 18px;">
            <div style="font-size:14px;font-weight:900;color:#111827;margin:0 0 10px 0;">
              [{esc(subject)}]
            </div>
            <ul style="margin:0;padding-left:18px;">
              {''.join(lis)}
            </ul>
          </div>
        """)

    body = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{esc(title)}</title>
</head>
<body style="margin:0;padding:0;background:#f6f7f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Noto Sans KR',Arial,sans-serif;color:#111827;">
  <div style="max-width:720px;margin:0 auto;padding:24px;">
    <div style="background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:18px 18px 14px;">
      <div style="font-size:18px;font-weight:800;line-height:1.3;">{esc(title)}</div>
      <div style="margin-top:6px;font-size:13px;color:#6b7280;">
        생성 시각: <span style="font-weight:600;">{esc(generated_at)} (KST)</span>
      </div>
    </div>
    <div style="margin-top:12px;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:14px 18px;">
      <div style="font-size:13px;color:#6b7280;">총 기사</div>
      <div style="font-size:22px;font-weight:900;margin-top:2px;">{total_count}건</div>
    </div>
    {keyword_block_html}
    {''.join(subject_sections)}
    <div style="margin-top:14px;padding:12px 4px;color:#9ca3af;font-size:12px;line-height:1.5;">
      이 메일은 자동 생성되었습니다.
    </div>
  </div>
</body>
</html>"""
    return body
