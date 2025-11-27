import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from snowflake.connector import connect
import plotly.express as px
import plotly.graph_objects as go

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Stablecoin Peg Monitor",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
        line-height: 1.6;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
    }
    .status-healthy {
        color: #28a745;
        font-weight: 600;
    }
    .status-warning {
        color: #ffc107;
        font-weight: 600;
    }
    .status-critical {
        color: #dc3545;
        font-weight: 600;
    }
    .status-normal {
        color: #28a745;
        font-weight: 600;
    }
    .status-unusual {
        color: #ffc107;
        font-weight: 600;
    }
    .status-outlier {
        color: #dc3545;
        font-weight: 600;
    }
    .sidebar-info {
        background-color: #e7f3ff;
        padding: 1rem;
        border-radius: 6px;
        margin: 1rem 0;
        font-size: 0.9rem;
        line-height: 1.5;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'snowflake_conn' not in st.session_state:
    st.session_state.snowflake_conn = None


@st.cache_resource
def get_snowflake_connection():
    """Create and cache Snowflake connection."""
    # Check for required environment variables
    required_vars = {
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE")
    }
    
    missing_vars = [var for var, value in required_vars.items() if value is None]
    if missing_vars:
        st.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        st.info("Please create a `.env` file in the project root with your Snowflake credentials.")
        return None
    
    try:
        conn = connect(
            user=required_vars["SNOWFLAKE_USER"],
            password=required_vars["SNOWFLAKE_PASSWORD"],
            account=required_vars["SNOWFLAKE_ACCOUNT"],
            warehouse=required_vars["SNOWFLAKE_WAREHOUSE"],
            database=required_vars["SNOWFLAKE_DATABASE"],
            schema=required_vars["SNOWFLAKE_SCHEMA"],
            role=required_vars["SNOWFLAKE_ROLE"]
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None


@st.cache_data(ttl=60)  # Cache for 1 minute
def load_peg_health_data(token: str = None):
    """Load peg health data from Snowflake."""
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            TIMESTAMP,
            TOKEN,
            PRICE,
            DEVIATION_FROM_PEG,
            DEVIATION_FROM_PEG_PCT,
            PEG_STATUS
        FROM stablecoin_peg_health
        """
        
        if token:
            query += f" WHERE TOKEN = '{token}'"
        
        query += " ORDER BY TIMESTAMP DESC LIMIT 1000"
        
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            df.columns = df.columns.str.lower()
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            df = df.sort_values('datetime')
        
        return df
    except Exception as e:
        st.error(f"Error loading peg health data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_zscore_data(token: str = None):
    """Load z-score data from Snowflake."""
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            TIMESTAMP,
            TOKEN,
            PRICE,
            DEVIATION_FROM_PEG,
            ROLLING_MEAN_24H,
            ROLLING_STDDEV_24H,
            ZSCORE,
            ZSCORE_STATUS
        FROM stablecoin_peg_zscore
        """
        
        if token:
            query += f" WHERE TOKEN = '{token}'"
        
        query += " ORDER BY TIMESTAMP DESC LIMIT 5000"
        
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            df.columns = df.columns.str.lower()
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        
        return df
    except Exception as e:
        st.error(f"Error loading z-score data: {str(e)}")
        return pd.DataFrame()


# Sidebar
with st.sidebar:
    st.markdown('<h1 style="font-size: 1.8rem; color: #1f77b4; margin-bottom: 0.5rem;">Stablecoin Peg Monitor</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Token selector
    selected_token = st.selectbox(
        "Select Token",
        options=["All", "USDC", "USDT"],
        index=0
    )
    
    token_filter = None if selected_token == "All" else selected_token
    
    st.markdown("---")
    
    # Connection status
    conn = get_snowflake_connection()
    if conn:
        st.success("Connected to Snowflake")
    else:
        st.error("Not connected to Snowflake")
    
    st.markdown("---")
    st.markdown("### Navigation")
    st.markdown("""
    - **Peg Deviation**: Real-time price deviation analysis
    - **Peg Z-Score**: Statistical anomaly detection
    """)


# Main content area
st.markdown('<div class="main-header">Stablecoin Peg Monitor</div>', unsafe_allow_html=True)
st.markdown("""
<div class="sub-header">
    Real-time monitoring and analysis of stablecoin peg stability for USDC and USDT. 
    This dashboard tracks price deviations from the $1.00 target peg using Chainlink oracle feeds 
    and provides statistical analysis to identify anomalies and unusual market conditions.
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# Page selection
page = st.selectbox(
    "Select Analysis View",
    ["Peg Deviation", "Peg Z-Score"],
    key="page_selector"
)

st.markdown("---")

# Page 1: Peg Deviation
if page == "Peg Deviation":
    st.header("Peg Deviation Analysis")
    st.markdown("""
    **Real-time Chainlink Price Feed Monitoring**
    
    This view displays the current price and deviation from the $1.00 target peg for selected stablecoins. 
    Prices are updated in real-time from Chainlink oracle feeds on the Ethereum mainnet.
    """)
    
    df_peg = load_peg_health_data(token_filter)
    
    if df_peg.empty:
        st.warning("No peg health data available. Please ensure data has been loaded into Snowflake.")
    else:
        # Filter out DAI if present
        df_peg = df_peg[df_peg['token'] != 'DAI']
        
        if token_filter:
            chart_data = df_peg[df_peg['token'] == token_filter].set_index('datetime')[['price', 'deviation_from_peg']]
        else:
            price_data = df_peg.pivot(index='datetime', columns='token', values='price')
            deviation_data = df_peg.pivot(index='datetime', columns='token', values='deviation_from_peg')
            chart_data = pd.DataFrame({
                'price': price_data.mean(axis=1) if len(price_data.columns) > 1 else price_data.iloc[:, 0],
                'deviation_from_peg': deviation_data.mean(axis=1) if len(deviation_data.columns) > 1 else deviation_data.iloc[:, 0]
            })
        
        if not chart_data.empty:
            # Current status metrics
            st.subheader("Current Status")
            col1, col2, col3, col4 = st.columns(4)
            
            latest_data = df_peg.groupby('token').last() if not token_filter else df_peg.iloc[-1:].set_index('token')
            
            if token_filter:
                latest = latest_data.loc[token_filter] if token_filter in latest_data.index else latest_data.iloc[0]
                with col1:
                    st.metric("Current Price", f"${latest['price']:.6f}")
                with col2:
                    st.metric("Deviation", f"{latest['deviation_from_peg']:.6f}")
                with col3:
                    st.metric("Deviation %", f"{latest['deviation_from_peg_pct']:.4f}%")
                with col4:
                    status = latest.get('peg_status', 'unknown')
                    status_class = {
                        'healthy': 'status-healthy',
                        'warning': 'status-warning',
                        'critical': 'status-critical'
                    }.get(status, '')
                    st.markdown(f'<div class="metric-card"><strong>Status:</strong><br><span class="{status_class}">{status.title()}</span></div>', unsafe_allow_html=True)
            else:
                for idx, (token, row) in enumerate(latest_data.iterrows()):
                    if idx < 4:
                        with [col1, col2, col3, col4][idx]:
                            st.metric(f"{token} Price", f"${row['price']:.6f}")
                            st.caption(f"Deviation: {row['deviation_from_peg_pct']:.4f}%")
            
            st.markdown("---")
            
            # Price chart
            st.subheader("Price Over Time")
            st.markdown("Historical price data from Chainlink oracle feeds")
            fig_price = go.Figure()
            if token_filter:
                fig_price.add_trace(go.Scatter(
                    x=chart_data.index,
                    y=chart_data['price'],
                    mode='lines',
                    name=token_filter,
                    line=dict(color='#1f77b4', width=2)
                ))
            else:
                for token in df_peg['token'].unique():
                    token_data = df_peg[df_peg['token'] == token].set_index('datetime')
                    fig_price.add_trace(go.Scatter(
                        x=token_data.index,
                        y=token_data['price'],
                        mode='lines',
                        name=token,
                        line=dict(width=2)
                    ))
            fig_price.update_layout(
                xaxis_title="Time",
                yaxis_title="Price (USD)",
                hovermode='x unified',
                height=400,
                template='plotly_white'
            )
            st.plotly_chart(fig_price, use_container_width=True)
            
            # Deviation chart
            st.subheader("Deviation from $1.00 Peg")
            st.markdown("Price deviation from the target peg value")
            fig_dev = go.Figure()
            if token_filter:
                fig_dev.add_trace(go.Scatter(
                    x=chart_data.index,
                    y=chart_data['deviation_from_peg'],
                    mode='lines',
                    name=token_filter,
                    line=dict(color='#d62728', width=2),
                    fill='tozeroy'
                ))
            else:
                for token in df_peg['token'].unique():
                    token_data = df_peg[df_peg['token'] == token].set_index('datetime')
                    fig_dev.add_trace(go.Scatter(
                        x=token_data.index,
                        y=token_data['deviation_from_peg'],
                        mode='lines',
                        name=token,
                        line=dict(width=2),
                        fill='tozeroy'
                    ))
            fig_dev.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Target Peg")
            fig_dev.update_layout(
                xaxis_title="Time",
                yaxis_title="Deviation from $1.00",
                hovermode='x unified',
                height=400,
                template='plotly_white'
            )
            st.plotly_chart(fig_dev, use_container_width=True)
            
            # Recent prices table
            st.subheader("Recent Price Updates")
            display_df = df_peg[['datetime', 'token', 'price', 'deviation_from_peg_pct', 'peg_status']].head(50)
            display_df.columns = ['Timestamp', 'Token', 'Price', 'Deviation %', 'Status']
            display_df['Timestamp'] = display_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.info("No data available for the selected token.")

# Page 2: Peg Z-Score
elif page == "Peg Z-Score":
    st.header("Peg Z-Score Analysis")
    st.markdown("""
    **Statistical Anomaly Detection**
    
    This view provides statistical analysis of peg deviations using z-scores calculated from 24-hour rolling statistics. 
    Z-scores help identify unusual price movements and potential market anomalies.
    """)
    
    df_zscore = load_zscore_data(token_filter)
    
    if df_zscore.empty:
        st.warning("No z-score data available. Please ensure data has been loaded into Snowflake.")
    else:
        # Filter out DAI if present
        df_zscore = df_zscore[df_zscore['token'] != 'DAI']
        
        if token_filter:
            st.subheader(f"Z-Score Analysis: {token_filter}")
            
            # Current z-score metrics
            latest = df_zscore[df_zscore['token'] == token_filter].iloc[-1]
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Current Z-Score", f"{latest['zscore']:.3f}")
            with col2:
                st.metric("Rolling Mean (24h)", f"{latest['rolling_mean_24h']:.6f}")
            with col3:
                st.metric("Rolling StdDev (24h)", f"{latest['rolling_stddev_24h']:.6f}")
            with col4:
                status = latest.get('zscore_status', 'unknown')
                status_class = {
                    'normal': 'status-normal',
                    'unusual': 'status-unusual',
                    'outlier': 'status-outlier'
                }.get(status, '')
                st.markdown(f'<div class="metric-card"><strong>Status:</strong><br><span class="{status_class}">{status.title()}</span></div>', unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Z-Score time series
            st.subheader("Z-Score Over Time")
            zscore_data = df_zscore[df_zscore['token'] == token_filter].set_index('datetime')
            fig_z = go.Figure()
            fig_z.add_trace(go.Scatter(
                x=zscore_data.index,
                y=zscore_data['zscore'],
                mode='lines',
                name='Z-Score',
                line=dict(color='#1f77b4', width=2)
            ))
            fig_z.add_hline(y=1, line_dash="dash", line_color="orange", annotation_text="±1σ Threshold")
            fig_z.add_hline(y=-1, line_dash="dash", line_color="orange")
            fig_z.add_hline(y=2, line_dash="dash", line_color="red", annotation_text="±2σ Threshold")
            fig_z.add_hline(y=-2, line_dash="dash", line_color="red")
            fig_z.add_hline(y=0, line_dash="dot", line_color="gray")
            fig_z.update_layout(
                xaxis_title="Time",
                yaxis_title="Z-Score",
                hovermode='x unified',
                height=400,
                template='plotly_white'
            )
            st.plotly_chart(fig_z, use_container_width=True)
            
        else:
            st.subheader("Z-Score Heatmap: All Tokens")
            
            # Create pivot table for heatmap
            pivot_data = df_zscore.pivot_table(
                index='datetime',
                columns='token',
                values='zscore',
                aggfunc='mean'
            )
            
            if not pivot_data.empty:
                fig = px.imshow(
                    pivot_data.T,
                    labels=dict(x="Time", y="Token", color="Z-Score"),
                    aspect="auto",
                    color_continuous_scale="RdBu_r",
                    color_continuous_midpoint=0
                )
                fig.update_layout(height=400, template='plotly_white')
                st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            
            # Z-score time series for all tokens
            st.subheader("Z-Score Time Series")
            zscore_chart = df_zscore.pivot(index='datetime', columns='token', values='zscore')
            fig_ts = go.Figure()
            for token in zscore_chart.columns:
                fig_ts.add_trace(go.Scatter(
                    x=zscore_chart.index,
                    y=zscore_chart[token],
                    mode='lines',
                    name=token,
                    line=dict(width=2)
                ))
            fig_ts.add_hline(y=1, line_dash="dash", line_color="orange")
            fig_ts.add_hline(y=-1, line_dash="dash", line_color="orange")
            fig_ts.add_hline(y=2, line_dash="dash", line_color="red")
            fig_ts.add_hline(y=-2, line_dash="dash", line_color="red")
            fig_ts.update_layout(
                xaxis_title="Time",
                yaxis_title="Z-Score",
                hovermode='x unified',
                height=400,
                template='plotly_white'
            )
            st.plotly_chart(fig_ts, use_container_width=True)
        
        st.markdown("---")
        
        # Z-score distribution
        st.subheader("Statistical Distribution")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Z-Score Distribution**")
            if token_filter:
                dist_data = df_zscore[df_zscore['token'] == token_filter]['zscore']
            else:
                dist_data = df_zscore['zscore']
            fig_dist = px.histogram(
                x=dist_data,
                nbins=30,
                labels={'x': 'Z-Score', 'y': 'Frequency'},
                template='plotly_white'
            )
            fig_dist.update_layout(height=300)
            st.plotly_chart(fig_dist, use_container_width=True)
        
        with col2:
            st.markdown("**Status Distribution**")
            if token_filter:
                status_counts = df_zscore[df_zscore['token'] == token_filter]['zscore_status'].value_counts()
            else:
                status_counts = df_zscore['zscore_status'].value_counts()
            fig_status = px.bar(
                x=status_counts.index,
                y=status_counts.values,
                labels={'x': 'Status', 'y': 'Count'},
                template='plotly_white',
                color=status_counts.index,
                color_discrete_map={
                    'normal': '#28a745',
                    'unusual': '#ffc107',
                    'outlier': '#dc3545'
                }
            )
            fig_status.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_status, use_container_width=True)
        
        # Recent z-scores table
        st.subheader("Recent Z-Score Data")
        display_df = df_zscore[['datetime', 'token', 'zscore', 'zscore_status', 'deviation_from_peg']].head(100)
        display_df.columns = ['Timestamp', 'Token', 'Z-Score', 'Status', 'Deviation']
        display_df['Timestamp'] = display_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.9rem; padding: 1rem 0;">
    <strong>Data Source:</strong> Snowflake Data Warehouse | 
    <strong>Price Feed:</strong> Chainlink Oracles (Ethereum Mainnet) | 
    <strong>Last Updated:</strong> Real-time
</div>
""", unsafe_allow_html=True)
