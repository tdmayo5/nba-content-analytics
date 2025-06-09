"""
NBA Content Analytics Dashboard
Interactive Streamlit dashboard for NBA content performance analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import numpy as np
from datetime import datetime, date, timedelta
import random

# Configure page
st.set_page_config(
    page_title="NBA Content Analytics",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .sidebar .sidebar-content {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_db_connection():
    """Get database connection with error handling"""
    try:
        return psycopg2.connect(
            host='postgres',
            database='nba_analytics',
            user='airflow',
            password='airflow',
            connect_timeout=10
        )
    except:
        # Fallback for development/demo purposes
        return None

@st.cache_data(ttl=300)
def load_sample_data():
    """Load sample data for demonstration"""
    # Generate sample game data
    teams = ['LAL', 'GSW', 'BOS', 'MIL', 'PHX', 'BRK', 'MIA', 'PHI', 'DEN', 'DAL']
    dates = pd.date_range(start='2024-01-01', end='2024-06-06', freq='D')
    
    games_data = []
    for i, game_date in enumerate(dates[:100]):  # 100 sample games
        home_team = random.choice(teams)
        away_team = random.choice([t for t in teams if t != home_team])
        
        # Generate realistic game data
        home_score = random.randint(95, 130)
        away_score = random.randint(95, 130)
        
        # TV ratings based on team popularity and game excitement
        base_rating = random.uniform(1.5, 4.5)
        if abs(home_score - away_score) <= 5:  # Close game
            base_rating *= 1.3
        if home_team in ['LAL', 'GSW', 'BOS'] or away_team in ['LAL', 'GSW', 'BOS']:
            base_rating *= 1.2
        
        tv_rating = min(8.0, base_rating)
        estimated_viewers = tv_rating * random.uniform(1.1, 1.4)
        
        # Social media engagement
        social_mentions = random.randint(500, 2500)
        if tv_rating > 3.0:
            social_mentions *= 1.5
        
        # Excitement score
        excitement = min(100, max(0, 
            (100 - abs(home_score - away_score) * 2) +  # Close game bonus
            (tv_rating * 10) +  # High rating bonus
            (social_mentions / 50)  # Social buzz bonus
        ))
        
        games_data.append({
            'game_id': f"{game_date.strftime('%Y%m%d')}{home_team}{away_team}",
            'game_date': game_date,
            'home_team': home_team,
            'away_team': away_team,
            'home_score': home_score,
            'away_score': away_score,
            'total_score': home_score + away_score,
            'score_difference': abs(home_score - away_score),
            'tv_rating': round(tv_rating, 2),
            'estimated_viewers': round(estimated_viewers, 1),
            'social_mentions': int(social_mentions),
            'excitement_score': round(excitement, 1),
            'is_close_game': abs(home_score - away_score) <= 5,
            'day_of_week': game_date.strftime('%A'),
            'is_weekend': game_date.weekday() >= 5
        })
    
    return pd.DataFrame(games_data)

def create_kpi_metrics(df):
    """Create KPI metrics section"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_rating = df['tv_rating'].mean()
        st.metric(
            label="📺 Avg TV Rating",
            value=f"{avg_rating:.2f}",
            delta=f"+{0.15:.2f} vs last month"
        )
    
    with col2:
        total_viewers = df['estimated_viewers'].sum()
        st.metric(
            label="👥 Total Viewers (M)",
            value=f"{total_viewers:.1f}M",
            delta=f"+{12.3:.1f}% vs last month"
        )
    
    with col3:
        avg_social = df['social_mentions'].mean()
        st.metric(
            label="💬 Avg Social Mentions",
            value=f"{avg_social:,.0f}",
            delta=f"+{8.7:.1f}% vs last month"
        )
    
    with col4:
        avg_excitement = df['excitement_score'].mean()
        st.metric(
            label="🔥 Avg Excitement Score",
            value=f"{avg_excitement:.1f}",
            delta=f"+{5.2:.1f} vs last month"
        )

def create_content_performance_chart(df):
    """Create content performance scatter plot"""
    fig = px.scatter(
        df,
        x='excitement_score',
        y='tv_rating',
        size='social_mentions',
        color='is_close_game',
        hover_data=['home_team', 'away_team', 'game_date', 'estimated_viewers'],
        title="Content Performance: TV Ratings vs Excitement Score",
        labels={
            'excitement_score': 'Excitement Score (0-100)',
            'tv_rating': 'TV Rating',
            'is_close_game': 'Close Game'
        },
        color_discrete_map={True: '#ff6b6b', False: '#4ecdc4'}
    )
    
    fig.update_layout(
        height=500,
        showlegend=True,
        legend=dict(title="Game Type")
    )
    
    return fig

def create_team_popularity_chart(df):
    """Create team popularity analysis"""
    # Calculate team metrics
    team_stats = []
    teams = df['home_team'].unique()
    
    for team in teams:
        team_games = df[(df['home_team'] == team) | (df['away_team'] == team)]
        avg_rating = team_games['tv_rating'].mean()
        avg_social = team_games['social_mentions'].mean()
        total_viewers = team_games['estimated_viewers'].sum()
        
        team_stats.append({
            'team': team,
            'avg_tv_rating': avg_rating,
            'avg_social_mentions': avg_social,
            'total_viewers': total_viewers,
            'games_played': len(team_games)
        })
    
    team_df = pd.DataFrame(team_stats).sort_values('avg_tv_rating', ascending=True)
    
    fig = px.bar(
        team_df,
        x='avg_tv_rating',
        y='team',
        orientation='h',
        title="Team Content Appeal (Average TV Rating)",
        labels={'avg_tv_rating': 'Average TV Rating', 'team': 'Team'},
        color='avg_tv_rating',
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=500)
    return fig

def create_audience_engagement_timeline(df):
    """Create timeline of audience engagement"""
    daily_metrics = df.groupby('game_date').agg({
        'tv_rating': 'mean',
        'social_mentions': 'sum',
        'estimated_viewers': 'sum',
        'excitement_score': 'mean'
    }).reset_index()
    
    # Create subplot with secondary y-axis
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('TV Ratings & Excitement Over Time', 'Social Media Engagement'),
        vertical_spacing=0.1
    )
    
    # TV Rating and Excitement
    fig.add_trace(
        go.Scatter(
            x=daily_metrics['game_date'],
            y=daily_metrics['tv_rating'],
            name='TV Rating',
            line=dict(color='#1f77b4', width=3)
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=daily_metrics['game_date'],
            y=daily_metrics['excitement_score'],
            name='Excitement Score',
            line=dict(color='#ff7f0e', width=2),
            yaxis='y2'
        ),
        row=1, col=1
    )
    
    # Social mentions
    fig.add_trace(
        go.Bar(
            x=daily_metrics['game_date'],
            y=daily_metrics['social_mentions'],
            name='Social Mentions',
            marker_color='#2ca02c'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=600,
        title_text="Audience Engagement Timeline",
        showlegend=True
    )
    
    return fig

def create_content_recommendations(df):
    """Generate content recommendations"""
    st.subheader("🎯 Content Strategy Recommendations")
    
    # High-performing matchups
    high_rating_games = df[df['tv_rating'] > df['tv_rating'].quantile(0.8)]
    popular_matchups = high_rating_games.groupby(['home_team', 'away_team']).size().reset_index(name='frequency')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**🔥 High-Performance Content Patterns:**")
        
        close_game_impact = df[df['is_close_game']]['tv_rating'].mean() - df[~df['is_close_game']]['tv_rating'].mean()
        st.write(f"• Close games boost ratings by **{close_game_impact:.2f} points** on average")
        
        weekend_impact = df[df['is_weekend']]['tv_rating'].mean() - df[~df['is_weekend']]['tv_rating'].mean()
        st.write(f"• Weekend games perform **{weekend_impact:.2f} points** better")
        
        high_excitement = df[df['excitement_score'] > 75]
        social_correlation = np.corrcoef(df['excitement_score'], df['social_mentions'])[0,1]
        st.write(f"• Excitement score correlates with social buzz (**{social_correlation:.2f}**)")
    
    with col2:
        st.write("**📈 Programming Recommendations:**")
        
        best_teams = df.groupby('home_team')['tv_rating'].mean().nlargest(3)
        st.write("• **Star Teams for Prime Time:**")
        for team, rating in best_teams.items():
            st.write(f"  - {team}: {rating:.2f} avg rating")
        
        st.write("• **Optimal Scheduling:**")
        st.write("  - Weekend afternoon games")
        st.write("  - Rivalry matchups in prime time")
        st.write("  - Close competitive games")

def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">🏀 NBA Content Performance Analytics</h1>', unsafe_allow_html=True)
    st.markdown("*Analyzing NBA games like TV content for maximum audience engagement*")
    
    # Sidebar filters
    st.sidebar.header("📊 Dashboard Filters")
    
    # Load data
    df = load_sample_data()
    
    # Date range filter
    min_date = df['game_date'].min().date()
    max_date = df['game_date'].max().date()
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Team filter
    selected_teams = st.sidebar.multiselect(
        "Filter by Teams",
        options=sorted(df['home_team'].unique()),
        default=[]
    )
    
    # Apply filters
    if len(date_range) == 2:
        start_date, end_date = date_range
        df = df[(df['game_date'].dt.date >= start_date) & (df['game_date'].dt.date <= end_date)]
    
    if selected_teams:
        df = df[(df['home_team'].isin(selected_teams)) | (df['away_team'].isin(selected_teams))]
    
    # Show data info
    st.sidebar.markdown(f"**Data Summary:**")
    st.sidebar.markdown(f"• Games analyzed: **{len(df)}**")
    st.sidebar.markdown(f"• Date range: **{df['game_date'].min().strftime('%Y-%m-%d')}** to **{df['game_date'].max().strftime('%Y-%m-%d')}**")
    st.sidebar.markdown(f"• Teams: **{df['home_team'].nunique()}**")
    
    # Main dashboard content
    if len(df) > 0:
        # KPI Metrics
        create_kpi_metrics(df)
        
        st.markdown("---")
        
        # Charts in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(create_content_performance_chart(df), use_container_width=True)
        
        with col2:
            st.plotly_chart(create_team_popularity_chart(df), use_container_width=True)
        
        # Timeline chart (full width)
        st.plotly_chart(create_audience_engagement_timeline(df), use_container_width=True)
        
        # Content recommendations
        create_content_recommendations(df)
        
        # Data table
        with st.expander("📋 View Raw Data"):
            st.dataframe(
                df[['game_date', 'home_team', 'away_team', 'tv_rating', 'estimated_viewers', 
                   'social_mentions', 'excitement_score']].sort_values('game_date', ascending=False),
                use_container_width=True
            )
    
    else:
        st.warning("No data available for the selected filters. Please adjust your selections.")
    
    # Footer
    st.markdown("---")
    st.markdown("*This dashboard demonstrates content analytics patterns similar to those used by media companies like NBCUniversal for programming decisions.*")

if __name__ == "__main__":
    main()