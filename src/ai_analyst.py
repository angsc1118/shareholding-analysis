# 2025-12-20 14:40:00: [Refactor] AI 分析模組
import os
import streamlit as st
import pandas as pd
import anthropic

def get_anthropic_client():
    api_key = None
    try:
        api_key = st.secrets["ANTHROPIC_API_KEY"]
    except (FileNotFoundError, KeyError):
        pass
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    return anthropic.Anthropic(api_key=api_key)

def generate_chip_analysis(stock_id: str, df: pd.DataFrame):
    client = get_anthropic_client()
    if not client:
        return "⚠️ 錯誤：未設定 ANTHROPIC_API_KEY。", ""

    recent_data = df.head(8).copy()
    
    cols_to_keep = [
        'date', 
        '收盤價', 
        '>1000張_比例', 
        '>1000張_人數', 
        '>400張_比例', 
        '>400張_人數', 
        '總股東數'
    ]
    
    valid_cols = [c for c in cols_to_keep if c in recent_data.columns]
    data_str = recent_data[valid_cols].to_markdown(index=False)

    system_prompt = """
    你是一位專業的台股籌碼分析師。
    
    分析核心邏輯：
    1. **鎖碼判讀**：若「大戶持股比例增加」且「大戶人數減少」，代表籌碼高度集中(鎖碼)，偏多。
    2. **散戶指標**：總股東數增加通常代表籌碼渙散，偏空。
    
    請提供簡短、條列式的繁體中文分析報告，字數 300 字以內。
    """

    user_message = f"""
    股票代號：{stock_id}
    近期籌碼數據 (由新到舊)：
    {data_str}
    
    請開始分析。
    """

    full_debug_log = f"""--- [System Prompt] ---\n{system_prompt}\n\n--- [User Message & Data] ---\n{user_message}"""

    try:
        message = client.messages.create(
            model="claude-3-5-sonnet-latest",
            max_tokens=1000,
            temperature=0.3,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}]
        )
        return message.content[0].text, full_debug_log

    except Exception as e:
        return f"❌ AI 分析連線失敗：{str(e)}", full_debug_log
