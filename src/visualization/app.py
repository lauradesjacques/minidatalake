import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page config
st.set_page_config(page_title="Data Lake Dashboard", layout="wide")

# Initialize DuckDB connection
db_path = "/app/minilake.duckdb"
con = duckdb.connect(db_path)

# Sidebar navigation
page = st.sidebar.radio("Choose Analysis", ["Data Overview", "YouTube Analysis", "Corona Analysis", "Control Panel"])

if page == "Data Overview":
    st.title("Data Lake Overview")
    
    # List all tables
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
    st.subheader("Available Tables")
    for table in tables:
        table_name = table[0]
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        st.write(f"- {table_name}: {row_count:,} rows")
        
        # Show sample data
        with st.expander(f"Sample data from {table_name}"):
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
            st.dataframe(sample)
            
            # Show column info
            st.write("Column Information:")
            for col in sample.columns:
                dtype = sample[col].dtype
                null_count = sample[col].isna().sum()
                st.write(f"- {col}: {dtype} (Nulls: {null_count})")

elif page == "YouTube Analysis":
    st.title("YouTube Trends Analysis")
    
    # Country selection
    country_tables = {
        'CA': 'youtube_cavideos',
        'DE': 'youtube_devideos',
        'FR': 'youtube_frvideos',
        'GB': 'youtube_gbvideos',
        'IN': 'youtube_invideos',
        'JP': 'youtube_jpvideos',
        'KR': 'youtube_krvideos',
        'MX': 'youtube_mxvideos',
        'RU': 'youtube_ruvideos',
        'US': 'youtube_usvideos'
    }
    
    # Category ID to Name mapping (partial, add more as needed)
    category_map = {
        1: 'Film & Animation', 2: 'Autos & Vehicles', 10: 'Music', 15: 'Pets & Animals',
        17: 'Sports', 18: 'Short Movies', 19: 'Travel & Events', 20: 'Gaming',
        21: 'Videoblogging', 22: 'People & Blogs', 23: 'Comedy', 24: 'Entertainment',
        25: 'News & Politics', 26: 'Howto & Style', 27: 'Education', 28: 'Science & Technology',
        29: 'Nonprofits & Activism', 30: 'Movies', 31: 'Anime/Animation', 32: 'Action/Adventure',
        33: 'Classics', 34: 'Comedy', 35: 'Documentary', 36: 'Drama', 37: 'Family',
        38: 'Foreign', 39: 'Horror', 40: 'Sci-Fi/Fantasy', 41: 'Thriller', 42: 'Shorts', 43: 'Shows', 44: 'Trailers'
    }
    
    selected_country = st.selectbox("Select Country", list(country_tables.keys()))
    table_name = country_tables[selected_country]
    
    # Load all data for summary metrics
    all_query = f"SELECT * FROM {table_name}"
    all_df = con.execute(all_query).fetchdf()
    all_df['category_name'] = all_df['category_id'].map(category_map).fillna(all_df['category_id'].astype(str))
    
    # --- Summary Metrics ---
    st.markdown("#### Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Videos", f"{len(all_df):,}")
    with col2:
        st.metric("Total Views", f"{all_df['views'].sum():,}")
    with col3:
        most_pop_cat = all_df['category_name'].value_counts().idxmax()
        st.metric("Most Popular Category", most_pop_cat)
    with col4:
        avg_eng = ((all_df['likes'] + all_df['comment_count']) / all_df['views']).mean() * 100
        st.metric("Avg. Engagement (%)", f"{avg_eng:.2f}")
    
    # --- Video Categories Analysis ---
    st.subheader("Video Categories Analysis")
    st.write("Distribution of videos and average views by category.")
    category_df = all_df.groupby('category_name').agg(
        video_count=('video_id', 'count'),
        avg_views=('views', 'mean'),
        avg_likes=('likes', 'mean'),
        avg_comments=('comment_count', 'mean')
    ).reset_index().sort_values('video_count', ascending=False)
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(category_df, x='category_name', y='video_count',
                    title='Number of Videos by Category',
                    labels={'category_name': 'Category', 'video_count': 'Number of Videos'},
                    color='video_count', color_continuous_scale='Blues')
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(category_df, x='category_name', y='avg_views',
                    title='Average Views by Category',
                    labels={'category_name': 'Category', 'avg_views': 'Average Views'},
                    color='avg_views', color_continuous_scale='Greens')
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # --- Top Channels ---
    st.subheader("Top Channels by Total Views")
    st.write("Channels with the highest total views.")
    top_channels = all_df.groupby('channel_title').agg(
        total_views=('views', 'sum'),
        video_count=('video_id', 'count')
    ).reset_index().sort_values('total_views', ascending=False).head(5)
    fig = px.bar(top_channels, x='total_views', y='channel_title', orientation='h',
                title='Top 5 Channels by Total Views',
                labels={'channel_title': 'Channel', 'total_views': 'Total Views'},
                color='total_views', color_continuous_scale='Oranges')
    st.plotly_chart(fig, use_container_width=True)
    
    # --- Trending Patterns Over Time ---
    st.subheader("Trending Videos Over Time")
    st.write("Number of trending videos published per month.")
    all_df['publish_time'] = pd.to_datetime(all_df['publish_time'], errors='coerce')
    all_df['month'] = all_df['publish_time'].dt.to_period('M').astype(str)
    trend_df = all_df.groupby('month').agg(video_count=('video_id', 'count')).reset_index()
    fig = px.line(trend_df, x='month', y='video_count',
                title='Trending Videos Published Per Month',
                labels={'month': 'Month', 'video_count': 'Number of Videos'})
    st.plotly_chart(fig, use_container_width=True)
    
    # --- Engagement Metrics ---
    st.subheader("Engagement Metrics")
    st.write("Top videos by views and by engagement rate.")
    all_df['engagement_rate'] = ((all_df['likes'] + all_df['comment_count']) / all_df['views']) * 100
    top_views = all_df.nlargest(10, 'views')
    top_engage = all_df.nlargest(10, 'engagement_rate')
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(top_views, x='views', y='title', orientation='h',
                    title='Top Videos by Views',
                    labels={'title': 'Video Title', 'views': 'Views'},
                    color='views', color_continuous_scale='Blues')
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.bar(top_engage, x='engagement_rate', y='title', orientation='h',
                    title='Top Videos by Engagement Rate',
                    labels={'title': 'Video Title', 'engagement_rate': 'Engagement Rate (%)'},
                    color='engagement_rate', color_continuous_scale='Purples')
        st.plotly_chart(fig, use_container_width=True)
    
    # --- Engagement Rate Distribution ---
    st.subheader("Engagement Rate Distribution")
    st.write("Distribution of engagement rates across all videos.")
    fig = px.box(all_df, y='engagement_rate', points='outliers',
                title='Engagement Rate Distribution',
                labels={'engagement_rate': 'Engagement Rate (%)'})
    st.plotly_chart(fig, use_container_width=True)

elif page == "Corona Analysis":
    st.title("COVID-19 Analysis")
    
    # 1. Global Overview
    st.subheader("Global Overview")
    global_query = """
    SELECT 
        SUM("TotalCases") as total_cases,
        SUM("TotalDeaths") as total_deaths,
        SUM("TotalRecovered") as total_recovered,
        SUM("ActiveCases") as active_cases
    FROM worldometer_data
    """
    global_stats = con.execute(global_query).fetchdf()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Cases", f"{global_stats['total_cases'][0]:,.0f}")
    with col2:
        st.metric("Total Deaths", f"{global_stats['total_deaths'][0]:,.0f}")
    with col3:
        st.metric("Total Recovered", f"{global_stats['total_recovered'][0]:,.0f}")
    with col4:
        st.metric("Active Cases", f"{global_stats['active_cases'][0]:,.0f}")
    
    # 2. Daily Trends
    st.subheader("Daily Global Trends")
    daily_query = """
    SELECT 
        "Date" as date,
        SUM("New cases") as new_cases,
        SUM("New deaths") as new_deaths,
        SUM("New recovered") as new_recovered
    FROM day_wise
    GROUP BY "Date"
    ORDER BY "Date"
    """
    daily_df = con.execute(daily_query).fetchdf()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily_df['date'], y=daily_df['new_cases'],
                            mode='lines', name='New Cases'))
    fig.add_trace(go.Scatter(x=daily_df['date'], y=daily_df['new_deaths'],
                            mode='lines', name='New Deaths'))
    fig.add_trace(go.Scatter(x=daily_df['date'], y=daily_df['new_recovered'],
                            mode='lines', name='New Recovered'))
    fig.update_layout(
        title='Daily Global COVID-19 Trends',
        xaxis_title='Date',
        yaxis_title='Count',
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # 3. Country-wise Analysis
    st.subheader("Country-wise Analysis")
    country_query = """
    SELECT 
        "Country/Region" as country_region,
        "TotalCases" as total_cases,
        "TotalDeaths" as total_deaths,
        "TotalRecovered" as total_recovered,
        "ActiveCases" as active_cases,
        "Population" as population,
        ("TotalCases"::FLOAT / NULLIF("Population",0) * 100) as cases_per_population
    FROM worldometer_data
    WHERE "Country/Region" != 'World'
    ORDER BY total_cases DESC
    LIMIT 20
    """
    country_df = con.execute(country_query).fetchdf()
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(country_df, x='country_region', y='total_cases',
                    title='Total Cases by Country (Top 20)',
                    labels={'country_region': 'Country', 'total_cases': 'Total Cases'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(country_df, x='country_region', y='cases_per_population',
                    title='Cases per Population (%) (Top 20)',
                    labels={'country_region': 'Country', 'cases_per_population': 'Cases per Population (%)'})
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    # 4. Recovery and Death Rates
    st.subheader("Recovery and Death Rates")
    rates_query = """
    SELECT 
        "Country/Region" as country_region,
        ("TotalRecovered"::FLOAT / NULLIF("TotalCases",0) * 100) as recovery_rate,
        ("TotalDeaths"::FLOAT / NULLIF("TotalCases",0) * 100) as death_rate
    FROM worldometer_data
    WHERE "TotalCases" > 1000
    ORDER BY "TotalCases" DESC
    LIMIT 20
    """
    rates_df = con.execute(rates_query).fetchdf()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=rates_df['country_region'], y=rates_df['recovery_rate'],
                        name='Recovery Rate'))
    fig.add_trace(go.Bar(x=rates_df['country_region'], y=rates_df['death_rate'],
                        name='Death Rate'))
    fig.update_layout(
        title='Recovery and Death Rates by Country (Top 20 by Cases)',
        xaxis_title='Country',
        yaxis_title='Rate (%)',
        barmode='group',
        xaxis_tickangle=-45
    )
    st.plotly_chart(fig, use_container_width=True)

elif page == "Control Panel":
    st.title("Control Panel")
    st.write("Upload a CSV file to insert into DuckDB or delete an existing table.")

    # --- Upload CSV and Insert into DuckDB ---
    st.subheader("Upload CSV and Insert into DuckDB")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Preview of uploaded CSV:")
        st.dataframe(df.head(5))
        new_table_name = st.text_input("Enter a new table name (e.g. my_new_table):")
        if new_table_name and st.button("Insert CSV into DuckDB"):
            con.execute(f"DROP TABLE IF EXISTS {new_table_name};")
            con.execute(f"CREATE TABLE {new_table_name} AS SELECT * FROM df;")
            st.success(f"CSV inserted into table {new_table_name}.")

    # --- List and Delete Existing Tables ---
    st.subheader("Existing Tables")
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    if not tables:
        st.write("No tables found.")
    else:
        for (table_name,) in tables:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"Table: {table_name}")
            with col2:
                if st.button(f"Delete {table_name}", key=f"del_{table_name}"):
                    con.execute(f"DROP TABLE {table_name};")
                    st.success(f"Table {table_name} deleted.")
                    st.experimental_rerun()

# Close the database connection
con.close() 