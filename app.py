# 2025-12-20 14:45:00: [Feat] 完整 App 介面
import streamlit as st
import pandas as pd
from src.database import get_latest_date, get_available_dates
from src.logic import calculate_top_growth, get_stock_distribution_table
from src.ai_analyst import generate_chip_analysis

st.set_page_config(
    page_title="台股籌碼戰情室",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

def format_stock_table(df: pd.DataFrame):
    styler = df.style
    columns_config = [
        ('總股東數', '總股東數_diff', '{:,.0f}'),
        ('平均張數/人', '平均張數/人_diff', '{:.2f}'),
        ('>400張_比例', '>400張_比例_diff', '{:.2f}%'),
        ('>400張_人數', '>400張_人數_diff', '{:,.0f}'),
        ('>1000張_比例', '>1000張_比例_diff', '{:.2f}%'),
        ('>1000張_人數', '>1000張_人數_diff', '{:,.0f}'),
        ('收盤價', '收盤價_diff', '{:.2f}')
    ]

    for col_name, diff_col, fmt in columns_config:
        if col_name in df.columns and diff_col in df.columns:
            styler = styler.format({col_name: fmt})
            def color_logic(row, c=col_name, d=diff_col):
                val = row[d]
                if pd.isna(val) or val == 0: return ''
                return 'color: #ff4b4b' if val > 0 else 'color: #28a745'
            
            styler = styler.apply(
                lambda x: [color_logic(x) if i == df.columns.get_loc(col_name) else '' for i in range(len(x))], 
                axis=1
            )

    hide_cols = [c for c in df.columns if c.endswith('_diff')]
    styler = styler.hide(subset=hide_cols, axis=1)
    return styler

with st.sidebar:
    st.title("⚙️ 系統控制台")
    latest_date = get_latest_date()
    st.info(f"📅 資料庫最新數據: **{latest_date}**")
    st.caption("Version: 1.3.0 (Fixed Data Logic)")

st.title("📊 台股籌碼資產戰情室")
tab1, tab2 = st.tabs(["🔥 大戶增減排行榜 (市場面)", "🔍 個股詳細分析 (技術面)"])

with tab1:
    st.header("🏆 千張大戶持股增減排行榜")
    dates = get_available_dates(limit=10)
    
    if len(dates) < 2:
        st.warning("⚠️ 資料庫數據不足兩週，請先執行資料回補。")
    else:
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: date_this = st.selectbox("選擇本期", dates, index=0)
        with col2: date_last = st.selectbox("選擇上期", dates, index=1 if len(dates)>1 else 0)
        with col3: 
            st.write("")
            run_btn = st.button("🚀 開始計算", use_container_width=True)

        if run_btn or date_this:
            with st.spinner("計算中..."):
                top_growth_df = calculate_top_growth(str(date_this), str(date_last))
                if not top_growth_df.empty:
                    st.dataframe(
                        top_growth_df,
                        use_container_width=True,
                        column_config={
                            "週增減%": st.column_config.NumberColumn(format="%.2f %%"),
                            "大戶持股比%": st.column_config.NumberColumn(format="%.2f %%"),
                            "持有股數": st.column_config.ProgressColumn(format="%d", min_value=0, max_value=int(top_growth_df['持有股數'].max()))
                        },
                        hide_index=True
                    )
                else:
                    st.info("查無資料。")

with tab2:
    st.header("📈 個股籌碼歷史趨勢")
    col_input, col_info = st.columns([1, 3])
    with col_input:
        target_stock = st.text_input("輸入股票代號", value="2330", max_chars=4)
    
    if target_stock and target_stock.isdigit() and len(target_stock)==4:
        with st.spinner(f"正在撈取 {target_stock} 資料..."):
            df_detail = get_stock_distribution_table(target_stock)
            
            if df_detail.empty:
                st.warning("查無資料 (可能為 ETF 或資料庫未更新)。")
            else:
                latest = df_detail.iloc[0]
                kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                
                def show_metric(col, label, key, diff_key, suffix=""):
                    val = latest.get(key, 0)
                    diff = latest.get(diff_key, 0)
                    col.metric(label, f"{val:,.2f}{suffix}" if suffix=="%" else f"{val:,.0f}", f"{diff:,.2f}{suffix}")

                show_metric(kpi1, "收盤價", "收盤價", "收盤價_diff")
                show_metric(kpi2, "總股東數", "總股東數", "總股東數_diff")
                show_metric(kpi3, "千張大戶比例", ">1000張_比例", ">1000張_比例_diff", "%")
                show_metric(kpi4, "千張大戶人數", ">1000張_人數", ">1000張_人數_diff")
                
                st.divider()
                st.subheader("📊 股價 vs 千張大戶持股比")
                chart_data = df_detail.sort_values('date', ascending=True).set_index('date')
                st.line_chart(chart_data[['>1000張_比例', '>400張_比例']])
                
                st.divider()
                st.subheader("🤖 AI 籌碼解讀 (Claude 3.5)")
                if st.button("⚡ 啟動 AI 智能分析"):
                    with st.spinner("連線分析中..."):
                        analysis, debug_prompt = generate_chip_analysis(target_stock, df_detail)
                        st.markdown(analysis)
                        with st.expander("🕵️ 開發者 Prompt 除錯"):
                            st.code(debug_prompt, language='markdown')

                st.divider()
                st.subheader("📋 詳細籌碼變化表")
                st.dataframe(format_stock_table(df_detail), use_container_width=True, height=500)
