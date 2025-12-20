# 2025-12-18 16:16:00: [Refactor] AI 分析模組：調整輸入欄位(加入大戶人數)、優化鎖碼邏輯 Prompt
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
        return "⚠️ 錯誤：未設定 ANTHROPIC_API_KEY，無法啟動 AI 分析師。", ""

    # 1. 資料前處理
    recent_data = df.head(8).copy()
    
    # [關鍵修改] 根據使用者需求調整欄位
    # 移除 '平均張數/人'
    # 加入 '>1000張_人數', '>400張_人數'
    cols_to_keep = [
        'date', 
        '收盤價', 
        '>1000張_比例', 
        '>1000張_人數', 
        '>400張_比例', 
        '>400張_人數', 
        '總股東數'
    ]
    
    # 確保欄位存在，避免 Key Error
    valid_cols = [c for c in cols_to_keep if c in recent_data.columns]
    
    # 轉為 Markdown，這就是 AI 看到的數據格式
    data_str = recent_data[valid_cols].to_markdown(index=False)

    # 2. 組合 Prompt (System Prompt 教導 AI 判讀人數變化)
    system_prompt = """
    你是一位專業的台股籌碼分析師。你的任務是根據使用者提供的「集保股權分散表」與「股價」歷史數據，撰寫一份簡短精準的分析報告。
    
    分析核心邏輯 (重要)：
    1. **大戶鎖碼判讀 (Locking Chips)**：
       - 請重點觀察「大戶持股比例」與「大戶人數」的關係。
       - 若 **「持股比例增加」且「人數減少」**：這是最強的偏多訊號，代表籌碼集中到極少數主力手中（鎖碼）。
       - 若「持股比例減少」且「人數增加」：代表主力將籌碼分散出貨給散戶或中實戶，偏空。
    
    2. **散戶指標**：
       - 觀察「總股東數」。若股價上漲但股東數大幅減少，代表散戶下車，籌碼安定。

    3. **結論判斷**：
       - 綜合以上數據，給出明確的「偏多」、「偏空」或「中性觀望」判斷。
       - 若發現上述「鎖碼」現象，請務必在報告中強調。

    輸出格式限制：
    - 請用繁體中文 (Traditional Chinese)。
    - 使用列點 (Bullet points)。
    - 字數控制在 300 字以內。
    """

    user_message = f"""
    股票代號：{stock_id}
    近期籌碼數據 (由新到舊)：
    {data_str}
    
    請開始分析。
    """

    # 3. 組合完整的 Debug 資訊
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
        return message.content[0].text, full_debug_log

    except Exception as e:
        return f"❌ AI 分析連線失敗：{str(e)}", full_debug_log
