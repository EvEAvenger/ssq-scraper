# -*- coding: utf-8 -*-
"""
改进版：优先用 一起彩(yiqicai) 作为数据源，并通过公开只读镜像通道 r.jina.ai
绕过运行器出口IP被拦的问题；解析每期详情页，抓取：
期号、开奖日期、红球1–6、蓝球、销售额(元)、奖池金额(元)、一等奖注数、
一等奖地区分布（原文）、一等奖地区分布（结构化JSON）、单省最高注数。

输出列顺序固定，便于后续建模。
"""

import os, re, json, time, argparse, requests, pandas as pd
from bs4 import BeautifulSoup

HDRS = {"User-Agent":"Mozilla/5.0"}
TIMEOUT = 15

def fetch_text(url: str) -> str:
    """先直连取网页，失败则走 r.jina.ai 只读镜像通道"""
    try:
        r = requests.get(url, headers=HDRS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception:
        pass
    # fallback via r.jina.ai
    if url.startswith("https://"):
        prox = "https://r.jina.ai/http/" + url[len("https://"):]
    elif url.startswith("http://"):
        prox = "https://r.jina.ai/http/" + url[len("http://"):]
    else:
        prox = "https://r.jina.ai/http/" + url
    r = requests.get(prox, headers=HDRS, timeout=TIMEOUT+5)
    r.raise_for_status()
    return r.text

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
    out = {}
    if not text: return out
    for seg in re.split(r"[，,；;、\s]+", text.strip("。 ")):
        m = re.search(r"([\u4e00-\u9fa5·]+)\s*(\d+)\s*注", seg)
        if m:
            prov, n = m.group(1), int(m.group(2))
            out[prov] = out.get(prov, 0) + n
    return out

def yiqicai_issue_list(limit=50):
    """从一起彩双色球列表页提取最近期号（通过镜像通道抓页面）"""
    url = "https://www.yiqicai.com/kj/ssqkj/"
    html = fetch_text(url)
    # 提取形如 2025xxxx 这样的期号，按出现顺序去重
    issues = re.findall(r"(20\d{5})\s*期", html)
    seen, out = set(), []
    for x in issues:
        if x not in seen:
            out.append(x); seen.add(x)
        if len(out) >= limit: break
    return out

def yiqicai_parse_issue(issue: str):
    """解析某一期的详情页"""
    url = f"https://www.yiqicai.com/kj/ssqkj/ssq_{issue}.html"
    html = fetch_text(url)
    soup = BeautifulSoup(html, "lxml")
    txt = soup.get_text(" ", strip=True)

    # 日期
    m_date = re.search(rf"{issue}\s*期\s*\[(\d{{4}}-\d{{2}}-\d{{2}})\]", txt)
    date = m_date.group(1) if m_date else None

    # 号码（6红+1蓝）
    # 页面中通常能直接匹配到六个两位数红球与一个两位数蓝球
    m_nums = re.search(r"(\d{2}\s+\d{2}\s+\d{2}\s+\d{2}\s+\d{2}\s+\d{2})\s+(\d{2})", html)
    reds, blue = [], None
    if m_nums:
        reds = [int(x) for x in re.findall(r"\d{2}", m_nums.group(1))][:6]
        blue = int(m_nums.group(2))

    # 销售额 / 奖池（不同文案做多种兜底）
    m_sales = (re.search(r"本期全国销量[:：]\s*([0-9\.,万亿]+)", txt) or
               re.search(r"本期销量[:：]\s*([0-9\.,万亿]+)", txt))
    m_pool  = (re.search(r"累计奖池[:：]\s*([0-9\.,万亿]+)", txt) or
               re.search(r"奖池金额[:：]\s*([0-9\.,万亿]+)", txt))
    sales = money_to_yuan(m_sales.group(1)) if m_sales else None
    pool  = money_to_yuan(m_pool.group(1)) if m_pool else None

    # 一等奖注数（表述可能是“一等奖 6+1 13”或“一等奖13注”等）
    m_fc = (re.search(r"一等奖\s*[（(]?\s*6\+1[)）]?\s*([0-9]+)", txt) or
            re.search(r"一等奖\s*([0-9]+)\s*注", txt))
    first_cnt = int(m_fc.group(1)) if m_fc else None

    # 一等奖地区分布原文（页面有“一等奖中奖明细：”段落）
    m_raw = re.search(r"一等奖中奖明细[:：]\s*(.*?)\s*彩种工具箱", txt)
    dist_raw = m_raw.group(1).strip() if m_raw else None

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
    ap.add_argument("--mode", choices=["recent"], default="recent")
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=50, help="最近N期")
    args = ap.parse_args()

    issues = yiqicai_issue_list(limit=args.limit)
    rows = []
    for iss in issues:
        d = yiqicai_parse_issue(iss)
        if d: rows.append(d)
        time.sleep(0.2)

    if not rows:
        raise RuntimeError("未抓到任何期次，可能被源站限流，请稍后再试。")

    df = pd.DataFrame(rows).sort_values("期号").reset_index(drop=True)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_excel(args.out, index=False)
    print(f"Saved {args.out} rows={len(df)}")

if __name__ == "__main__":
    main()
