# 2025-12-18 15:30:00: [Feat] AI 分析模組：整合 Anthropic Claude 3.5 進行籌碼解讀
import os
import streamlit as st
import pandas as pd
import anthropic

def get_anthropic_client():
    """初始化 Anthropic Client (支援 Secrets 與 環境變數)"""
    api_key = None
    # 1. 嘗試從 Streamlit Secrets 讀取
    try:
        api_key = st.secrets["ANTHROPIC_API_KEY"]
    except (FileNotFoundError, KeyError):
        pass
    
    # 2. 嘗試從環境變數讀取
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
    
    if not api_key:
        return None
        
    return anthropic.Anthropic(api_key=api_key)

def generate_chip_analysis(stock_id: str, df: pd.DataFrame) -> str:
    """
    將籌碼數據傳送給 Claude 進行分析
    Args:
        stock_id: 股票代號
        df: 包含日期、股價、大戶持股比的 DataFrame (已轉置過的寬表格)
    """
    client = get_anthropic_client()
    if not client:
        return "⚠️ 錯誤：未設定 ANTHROPIC_API_KEY，無法啟動 AI 分析師。"

    # 1. 資料前處理：只取最近 8 週數據，避免 Token 消耗過多
    # 假設 df 已經是按日期倒序 (最新的在上面)，我們取前 8 列
    recent_data = df.head(8).copy()
    
    # 將數據轉為 Markdown 表格字串，讓 AI 好讀
    # 只保留關鍵欄位以節省 Token
    cols_to_keep = ['date', '收盤價', '>1000張_比例', '>400張_比例', '總股東數', '平均張數/人']
    # 確保欄位存在 (防呆)
    valid_cols = [c for c in cols_to_keep if c in recent_data.columns]
    data_str = recent_data[valid_cols].to_markdown(index=False)

    # 2. Prompt Engineering (關鍵：賦予 AI 專業分析師的人設)
    system_prompt = """
    你是一位專業的台股籌碼分析師。你的任務是根據使用者提供的「集保股權分散表」與「股價」歷史數據，撰寫一份簡短精準的分析報告。
    
    分析重點：
    1. **大戶動向**：千張大戶持股比例是增加還是減少？是連續買進還是調節？
    2. **散戶指標**：總股東數與平均張數的變化。股東數增加通常代表籌碼渙散(不利)，反之則集中(有利)。
    3. **價量關係**：股價與大戶持股是否背離？(例如：股價跌但大戶拼命接，或是股價漲但大戶在倒貨)。
    4. **結論**：給出一個明確的「偏多」、「偏空」或「中性觀望」判斷，並說明風險。

    輸出限制：
    - 請用繁體中文 (Traditional Chinese)。
    - 使用列點 (Bullet points) 呈現。
    - 語氣專業、客觀，不要有多餘的寒暄。
    - 字數控制在 300 字以內。
    """

    user_message = f"""
    股票代號：{stock_id}
    近期籌碼數據 (由新到舊)：
    {data_str}
    
    請開始分析。
    """

    # 3. 呼叫 API (使用 Claude 3.5 Sonnet 或 Haiku)
    try:
        message = client.messages.create(
            model="claude-3-5-sonnet-latest", # 若想省錢可用 claude-3-haiku-20240307
            max_tokens=1000,
            temperature=0.3, # 低溫度以確保分析客觀穩定
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_message}
            ]
        )
        return message.content[0].text

    except Exception as e:
        return f"❌ AI 分析連線失敗：{str(e)}"
