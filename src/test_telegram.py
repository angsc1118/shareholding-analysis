# 2025-12-28 15:00:00: [Test] Telegram 連線測試腳本
import os
import sys
import requests

def send_test_msg():
    # 1. 從環境變數讀取 Secrets
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("❌ 錯誤: 缺少環境變數 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID")
        sys.exit(1)

    print(f"🤖 正在嘗試發送訊息給 Chat ID: {chat_id} ...")

    # 2. 呼叫 Telegram API
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "🚀 [GitHub Actions] 台股戰情室：連線測試成功！\n這是一則來自雲端的測試訊息。"
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        
        if resp.status_code == 200:
            print("✅ 測試成功！訊息已發送，請檢查手機 Telegram。")
        else:
            print(f"❌ 發送失敗 (HTTP {resp.status_code}): {resp.text}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ 連線錯誤: {e}")
        sys.exit(1)

if __name__ == "__main__":
    send_test_msg()
