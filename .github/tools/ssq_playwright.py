# -*- coding: utf-8 -*-
"""
用 Playwright (Chromium) 抓取最近 N 期双色球（主源：一起彩），
解析：期号、开奖日期、红1-红6、蓝、销售额(元)、奖池金额(元)、一等奖注数、
一等奖地区分布（原文/结构化JSON）、单省最高注数。
"""

import re, json, time, os, argparse, pandas as pd
from playwright.sync_api import sync_playwright

def money_to_yuan(txt):
    if not txt: return None
    t = txt.replace(",", "").strip()
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*亿", t)
    if m: return int(round(float(m.group(1))*1e8))
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*万", t)
    if m: return int(round(float(m.group(1))*1e4))
    m = re.search(r"([0-9]+)", t)
    return int(m.group(1)) if m else None

def parse_dist(text):
    out={}
    if not text: return out
    for seg in re.split(r"[，,；;、\s]+", text.strip("。 ")):
        m = re.search(r"([\u4e00-\u9fa5·]+)\s*(\d+)\s*注", seg)
        if m:
            prov, n = m.group(1), int(m.group(2))
            out[prov] = out.get(prov, 0) + n
    return out

def get_page_text(page, url, wait_ms=1200):
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(wait_ms)
    # 尽量拿到渲染后的文本
    try:
        return page.inner_text("body", timeout=5000)
    except:
        return page.content()

def yiqicai_issue_list(page, limit=50):
    txt = get_page_text(page, "https://www.yiqicai.com/kj/ssqkj/", wait_ms=1500)
    issues = re.findall(r"(20\d{5})\s*期", txt)
    seen=set(); out=[]
    for x in issues:
        if x not in seen:
            out.append(x); seen.add(x)
        if len(out) >= limit: break
    return out

def yiqicai_parse_issue(page, issue):
    url = f"https://www.yiqicai.com/kj/ssqkj/ssq_{issue}.html"
    html = get_page_text(page, url, wait_ms=1500)

    # 日期
    m_date = re.search(rf"{issue}\s*期\s*\[(\d{{4}}-\d{{2}}-\d{{2}})\]", html)
    date = m_date.group(1) if m_date else None

    # 号码：6红+1蓝（两位数）
    m_nums = re.search(r"(\d{2}\s+\d{2}\s+\d{2}\s+\d{2}\s+\d{2}\s+\d{2})\s+(\d{2})", html)
    reds, blue = [], None
    if m_nums:
        reds = [int(x) for x in re.findall(r"\d{2}", m_nums.group(1))][:6]
        blue = int(m_nums.group(2))

    # 销售额 / 奖池
    m_sales = re.search(r"(?:本期全国销量|本期销量)[:：]\s*([0-9\.,万亿]+)", html)
    m_pool  = re.search(r"(?:累计奖池|奖池金额)[:：]\s*([0-9\.,万亿]+)", html)
    sales = money_to_yuan(m_sales.group(1)) if m_sales else None
    pool  = money_to_yuan(m_pool.group(1)) if m_pool else None

    # 一等奖注数
    m_fc = (re.search(r"一等奖\s*[（(]?\s*6\+1[)）]?\s*([0-9]+)", html) or
            re.search(r"一等奖\s*([0-9]+)\s*注", html))
    first_cnt = int(m_fc.group(1)) if m_fc else None

    # 一等奖地区分布原文
    m_raw = re.search(r"一等奖中奖明细[:：]\s*(.*?)\s*(?:彩种工具箱|$)", html, flags=re.S)
    dist_raw = m_raw.group(1).strip() if m_raw else None
    # 如果没有，某些期会写在“开奖公告”段里，再兜底找一遍
    if not dist_raw:
        m2 = re.search(r"(?:中奖地|中奖地区|一等奖中奖地)[:：]\s*(.*?)\s*(?:，|。| |$)", html)
        dist_raw = m2.group(1).strip() if m2 else None

    dist = parse_dist(dist_raw or "")
    dist_json = json.dumps(dist, ensure_ascii=False, separators=(",",":")) if dist else None
    max_one = max(dist.values()) if dist else None

    if not (date and len(reds)==6 and isinstance(blue,int)):
        return None

    return {
        "期号": issue, "开奖日期": date,
        "红1": reds[0], "红2": reds[1], "红3": reds[2], "红4": reds[3], "红5": reds[4], "红6": reds[5],
        "蓝": blue,
        "销售额(元)": sales, "奖池金额(元)": pool, "一等奖注数": first_cnt,
        "一等奖地区分布（原文）": dist_raw,
        "一等奖地区分布（结构化JSON）": dist_json,
        "单省最高注数": max_one
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent="Mozilla/5.0")
        page = ctx.new_page()

        issues = yiqicai_issue_list(page, limit=args.limit)
        rows = []
        for iss in issues:
            d = yiqicai_parse_issue(page, iss)
            if d: rows.append(d)
            time.sleep(0.15)

        browser.close()

    if not rows:
        raise RuntimeError("未抓到任何期次（浏览器版）。可能仍被源站限流，请稍后重试。")

    df = pd.DataFrame(rows).sort_values("期号").reset_index(drop=True)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_excel(args.out, index=False)
    print(f"Saved {args.out} rows={len(df)}")

if __name__ == "__main__":
    main()
