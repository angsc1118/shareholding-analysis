# 2025-12-18 16:40:00: [Refactor] 修改回傳格式，同時回傳 Prompt 供除錯
import os
import streamlit as st
import pandas as pd
import anthropic

def get_anthropic_client():
    """初始化 Anthropic Client"""
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
    """
    回傳: (分析結果字串, 完整Prompt除錯資訊)
    """
    client = get_anthropic_client()
    if not client:
        return "⚠️ 錯誤：未設定 ANTHROPIC_API_KEY。", ""

    # 1. 資料前處理
    recent_data = df.head(8).copy()
    cols_to_keep = ['date', '收盤價', '>1000張_比例', '>400張_比例', '總股東數', '平均張數/人']
    valid_cols = [c for c in cols_to_keep if c in recent_data.columns]
    
    # 轉為 Markdown，這就是 AI 看到的數據格式
    data_str = recent_data[valid_cols].to_markdown(index=False)

    # 2. 組合 Prompt
    system_prompt = """
    你是一位專業的台股籌碼分析師。你的任務是根據使用者提供的「集保股權分散表」與「股價」歷史數據，撰寫一份簡短精準的分析報告。
    
    分析重點：
    1. **大戶動向**：千張大戶持股比例是增加還是減少？是連續買進還是調節？
    2. **散戶指標**：總股東數與平均張數的變化。
    3. **價量關係**：股價與大戶持股是否背離？
    4. **結論**：給出一個明確的「偏多」、「偏空」或「中性觀望」判斷。

    輸出限制：
    - 請用繁體中文。
    - 使用列點 (Bullet points)。
    - 字數控制在 300 字以內。
    """

    user_message = f"""
    股票代號：{stock_id}
    近期籌碼數據 (由新到舊)：
    {data_str}
    
    請開始分析。
    """

    # 3. 組合完整的 Debug 資訊 (這就是您想看到的內容)
    full_debug_log = f"""--- [System Prompt] ---\n{system_prompt}\n\n--- [User Message & Data] ---\n{user_message}"""

    # 4. 呼叫 API
    try:
        message = client.messages.create(
            model="claude-3-5-sonnet-latest",
            max_tokens=1000,
            temperature=0.3,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_message}
            ]
        )
        # 回傳 tuple: (結果, Prompt)
        return message.content[0].text, full_debug_log

    except Exception as e:
        return f"❌ AI 分析連線失敗：{str(e)}", full_debug_log
