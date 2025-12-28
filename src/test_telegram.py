import streamlit as st
import requests

# 讀取設定
try:
    TOKEN = st.secrets["TELEGRAM_BOT_TOKEN"]
    CHAT_ID = st.secrets["TELEGRAM_CHAT_ID"]
except:
    print("❌ 找不到 Secrets，請檢查 .streamlit/secrets.toml")
    exit()

def send_test_msg():
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": "🚀 台股戰情室：連線測試成功！(Hello from Python)"
    }
    resp = requests.post(url, json=payload)
    
    if resp.status_code == 200:
        print("✅ 測試訊息發送成功！請檢查手機 Telegram。")
    else:
        print(f"❌ 發送失敗: {resp.text}")

if __name__ == "__main__":
    send_test_msg()
