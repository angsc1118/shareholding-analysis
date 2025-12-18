# 2025-12-18 11:00:00: [Feat] 前端介面：整合市場排行與個股詳細分析 (UI/UX 優化版)
import streamlit as st
import pandas as pd
from src.database import get_latest_date, get_available_dates
from src.logic import calculate_top_growth, get_stock_distribution_table

# --- 1. 頁面全域設定 (Page Config) ---
st.set_page_config(
    page_title="台股籌碼戰情室",
    page_icon="📈",
    layout="wide", # 使用寬版面以容納詳細表格
    initial_sidebar_state="expanded"
)

# --- 2. 工具函式 (Helper Functions) ---

def apply_color_style(val):
    """
    通用著色邏輯：
    數值 > 0 -> 紅色
    數值 < 0 -> 綠色
    數值 = 0 -> 黑色/預設
    """
    if isinstance(val, (int, float)):
        if val > 0:
            return 'color: #ff4b4b; font-weight: bold;' # Streamlit Red
        elif val < 0:
            return 'color: #0df768; font-weight: bold;' # Streamlit Green (Bright)
    return ''

def format_stock_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """
    針對「個股詳細籌碼表」進行精緻化排版：
    1. 隱藏 Diff 輔助欄位
    2. 根據 Diff 欄位決定主要欄位的文字顏色 (紅漲綠跌)
    3. 設定數字格式 (千分位、百分比)
    """
    # 建立 Pandas Styler
    styler = df.style

    # 定義要顯示的欄位與其對應的 Diff 欄位
    # 格式: (顯示欄位, Diff欄位, 格式字串)
    columns_config = [
        ('總股東數', '總股東數_diff', '{:,.0f}'),
        ('平均張數/人', '平均張數/人_diff', '{:.2f}'),
        ('>400張_比例', '>400張_比例_diff', '{:.2f}%'),
        ('>1000張_人數', '>1000張_人數_diff', '{:.0f}'),
        ('>1000張_比例', '>1000張_比例_diff', '{:.2f}%'),
        ('收盤價', '收盤價_diff', '{:.2f}')
    ]

    # --- 核心邏輯：條件格式化 ---
    # 因為 Pandas Styler 的 apply 比較複雜，這裡我們用 apply loop 處理每一對欄位
    for col_name, diff_col, fmt in columns_config:
        if col_name in df.columns and diff_col in df.columns:
            # 1. 設定數值格式
            styler = styler.format({col_name: fmt})
            
            # 2. 設定顏色 (根據 diff_col 的值來改變 col_name 的顏色)
            def color_logic(row, c=col_name, d=diff_col):
                val = row[d]
                if pd.isna(val) or val == 0:
                    return ''
                return 'color: #ff4b4b' if val > 0 else 'color: #28a745' # Red Up, Green Down
            
            # 使用 apply(axis=1) 逐行處理
            styler = styler.apply(
                lambda x: [color_logic(x) if i == df.columns.get_loc(col_name) else '' for i in range(len(x))], 
                axis=1
            )

    # 隱藏所有以 _diff 結尾的欄位
    hide_cols = [c for c in df.columns if c.endswith('_diff')]
    styler = styler.hide(subset=hide_cols, axis=1)

    return styler

# --- 3. 側邊欄 (Sidebar) ---
with st.sidebar:
    st.title("⚙️ 系統控制台")
    
    latest_date = get_latest_date()
    st.info(f"📅 資料庫最新數據: **{latest_date}**")
    
    st.markdown("---")
    st.markdown("### 關於系統")
    st.caption("本系統整合集保結算所 (TDCC) 每週股權分散數據與 Yahoo Finance 股價，提供大戶籌碼動向分析。")
    st.caption("Version: 1.0.0 (Beta)")

# --- 4. 主頁面 (Main Content) ---
st.title("📊 台股籌碼資產戰情室")

# 建立分頁
tab1, tab2 = st.tabs(["🔥 大戶增減排行榜 (市場面)", "🔍 個股詳細分析 (技術面)"])

# ==========================================
# Tab 1: 市場大戶排行
# ==========================================
with tab1:
    st.header("🏆 千張大戶持股增減排行榜")
    
    # 取得可用日期
    dates = get_available_dates(limit=10)
    
    if len(dates) < 2:
        st.warning("⚠️ 資料庫數據不足兩週，無法計算比較。請先執行資料回補。")
    else:
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            date_this = st.selectbox("選擇本期 (This Week)", dates, index=0)
        with col2:
            # 預設選上一週
            default_last_idx = 1 if len(dates) > 1 else 0
            date_last = st.selectbox("選擇上期 (Last Week)", dates, index=default_last_idx)
        with col3:
            st.write("") # Spacer
            run_btn = st.button("🚀 開始計算", use_container_width=True)

        if run_btn or date_this:
            with st.spinner("正在計算全市場數據..."):
                top_growth_df = calculate_top_growth(str(date_this), str(date_last))
                
                if not top_growth_df.empty:
                    # 使用 Column Config 顯示精美進度條
                    st.dataframe(
                        top_growth_df,
                        use_container_width=True,
                        column_config={
                            "股票代號": st.column_config.TextColumn("代號"),
                            "大戶持股比%": st.column_config.NumberColumn(
                                "大戶持股比 (%)", format="%.2f %%"
                            ),
                            "週增減%": st.column_config.NumberColumn(
                                "週增減 (%)", format="%.2f %%", 
                            ),
                            "持有股數": st.column_config.ProgressColumn(
                                "持有股數 (視覺化)", format="%d", min_value=0, max_value=int(top_growth_df['持有股數'].max())
                            )
                        },
                        hide_index=True
                    )
                else:
                    st.info("查無資料，請確認日期區間。")

# ==========================================
# Tab 2: 個股詳細分析
# ==========================================
with tab2:
    st.header("📈 個股籌碼歷史趨勢")
    
    col_input, col_info = st.columns([1, 3])
    with col_input:
        target_stock = st.text_input("輸入股票代號 (例如 2330)", value="2330", max_chars=4)
    
    if target_stock:
        # 簡單驗證
        if not target_stock.isdigit() or len(target_stock) != 4:
            st.error("請輸入正確的 4 碼數字代號。")
        else:
            with st.spinner(f"正在撈取 {target_stock} 歷史資料..."):
                df_detail = get_stock_distribution_table(target_stock)
                
                if df_detail.empty:
                    st.warning(f"找不到 {target_stock} 的資料。可能是 ETF 或資料庫尚未更新。")
                else:
                    # 1. 顯示 KPI 指標 (最新一週)
                    latest = df_detail.iloc[0] # 因為 logic.py 已經依日期倒序排列，第 0 筆是最新的
                    
                    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                    
                    # 輔助函式：產生漂亮的 metric delta
                    def show_metric(col, label, val_key, diff_key, suffix=""):
                        val = latest.get(val_key, 0)
                        diff = latest.get(diff_key, 0)
                        # 處理 NaN
                        if pd.isna(val): val = 0
                        if pd.isna(diff): diff = 0
                        
                        col.metric(
                            label=label,
                            value=f"{val:,.2f}{suffix}" if suffix == "%" else f"{val:,.0f}",
                            delta=f"{diff:,.2f}{suffix}",
                            delta_color="normal" # Streamlit 自動判斷：正紅負綠 (需在 config 設定，但預設是 正綠負紅，我們用 inverse?)
                            # Streamlit 預設: Green=Up, Red=Down. 
                            # 台股習慣: Red=Up. 所以這裡單獨看可能會有文化差異
                            # 解決方案：我們在表格已經手動處理顏色，這裡先用預設，或不顯示顏色只顯示箭頭
                        )

                    show_metric(kpi1, "收盤價", "收盤價", "收盤價_diff")
                    show_metric(kpi2, "總股東數", "總股東數", "總股東數_diff")
                    show_metric(kpi3, "千張大戶比例", ">1000張_比例", ">1000張_比例_diff", "%")
                    show_metric(kpi4, "千張大戶人數", ">1000張_人數", ">1000張_人數_diff")
                    
                    st.divider()

                    # 2. 繪製圖表 (雙軸圖：股價 vs 大戶比例)
                    # 為了畫圖，需將日期轉回 index 並且排序為 舊->新
                    chart_data = df_detail.sort_values('date', ascending=True).set_index('date')
                    
                    st.subheader("📊 股價 vs 千張大戶持股比 走勢")
                    
                    # 使用 Streamlit 簡單圖表，或用 Altair/Plotly 做雙軸
                    # 這裡示範簡單版：直接用 st.line_chart (會畫在同一軸，比例尺不同較難看)
                    # 建議：顯示兩個簡單圖表上下排列，或使用 st.bar_chart + st.line_chart
                    
                    # 這裡我們用 Metric 呈現重點，圖表先畫大戶比例
                    st.line_chart(chart_data[['>1000張_比例', '>400張_比例']])

                    st.divider()

                    # 3. 詳細數據表格 (套用紅漲綠跌樣式)
                    st.subheader("📋 詳細籌碼變化表")
                    
                    # 套用我們寫好的 Styler
                    styled_df = format_stock_table(df_detail)
                    
                    st.dataframe(
                        styled_df,
                        use_container_width=True,
                        height=500 # 固定高度讓使用者可以捲動
                    )
