# NBA Analytics Platform - GCP Infrastructure
# Optimized for free tier usage

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "nba-analytics-free"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

# BigQuery datasets
resource "google_bigquery_dataset" "bronze_nba" {
  dataset_id = "bronze_nba"
  location   = "US"
  
  labels = {
    env     = "production"
    layer   = "bronze"
    project = "nba-analytics"
  }
}

resource "google_bigquery_dataset" "silver_nba" {
  dataset_id = "silver_nba"
  location   = "US"
  
  labels = {
    env     = "production" 
    layer   = "silver"
    project = "nba-analytics"
  }
}

resource "google_bigquery_dataset" "gold_nba" {
  dataset_id = "gold_nba"
  location   = "US"
  
  labels = {
    env     = "production"
    layer   = "gold" 
    project = "nba-analytics"
  }
}

# Cloud Storage bucket
resource "google_storage_bucket" "nba_analytics_data" {
  name     = "${var.project_id}-data"
  location = "US"
  
  # Lifecycle management to control costs
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
  
  versioning {
    enabled = false  # Disable to save storage costs
  }
}

# Compute Engine instance (always free f1-micro)
resource "google_compute_instance" "airflow_instance" {
  name         = "airflow-free"
  machine_type = "f1-micro"  # Always free tier
  zone         = "${var.region}-a"
  
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 30  # GB - within free tier
    }
  }
  
  network_interface {
    network = "default"
    access_config {
      // Ephemeral public IP
    }
  }
  
  metadata_startup_script = file("${path.module}/startup-script.sh")
  
  tags = ["airflow", "nba-analytics"]
  
  labels = {
    env     = "production"
    purpose = "airflow-orchestration"
  }
}

# Firewall rule for Airflow
resource "google_compute_firewall" "airflow_web" {
  name    = "allow-airflow-web"
  network = "default"
  
  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }
  
  source_ranges = ["0.0.0.0/0"]  # Restrict this in production
  target_tags   = ["airflow"]
}

# Output important information
output "bigquery_datasets" {
  value = {
    bronze = google_bigquery_dataset.bronze_nba.dataset_id
    silver = google_bigquery_dataset.silver_nba.dataset_id
    gold   = google_bigquery_dataset.gold_nba.dataset_id
  }
}

output "storage_bucket" {
  value = google_storage_bucket.nba_analytics_data.name
}

output "airflow_instance_ip" {
  value = google_compute_instance.airflow_instance.network_interface[0].access_config[0].nat_ip
}
def _parse_game_date(self, date_str: str) -> datetime.date:
    """Parse game date from API response"""
    try:
        if 'T' in date_str:
            return datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
        else:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        return datetime.now().date()

def _get_team_abbreviation(self, team_id: str) -> str:
    """Convert team ID to abbreviation"""
    return self.team_info.get(str(team_id), {}).get('abbreviation', 'UNK')

def _get_team_info(self) -> Dict:
    """NBA team information mapping"""
    return {
        '1610612737': {'abbreviation': 'ATL', 'name': 'Atlanta Hawks'},
        '1610612738': {'abbreviation': 'BOS', 'name': 'Boston Celtics'},
        '1610612751': {'abbreviation': 'BRK', 'name': 'Brooklyn Nets'},
        '1610612766': {'abbreviation': 'CHA', 'name': 'Charlotte Hornets'},
        '1610612741': {'abbreviation': 'CHI', 'name': 'Chicago Bulls'},
        '1610612739': {'abbreviation': 'CLE', 'name': 'Cleveland Cavaliers'},
        '1610612742': {'abbreviation': 'DAL', 'name': 'Dallas Mavericks'},
        '1610612743': {'abbreviation': 'DEN', 'name': 'Denver Nuggets'},
        '1610612765': {'abbreviation': 'DET', 'name': 'Detroit Pistons'},
        '1610612744': {'abbreviation': 'GSW', 'name': 'Golden State Warriors'},
        '1610612745': {'abbreviation': 'HOU', 'name': 'Houston Rockets'},
        '1610612754': {'abbreviation': 'IND', 'name': 'Indiana Pacers'},
        '1610612746': {'abbreviation': 'LAC', 'name': 'LA Clippers'},
        '1610612747': {'abbreviation': 'LAL', 'name': 'Los Angeles Lakers'},
        '1610612763': {'abbreviation': 'MEM', 'name': 'Memphis Grizzlies'},
        '1610612748': {'abbreviation': 'MIA', 'name': 'Miami Heat'},
        '1610612749': {'abbreviation': 'MIL', 'name': 'Milwaukee Bucks'},
        '1610612750': {'abbreviation': 'MIN', 'name': 'Minnesota Timberwolves'},
        '1610612740': {'abbreviation': 'NOP', 'name': 'New Orleans Pelicans'},
        '1610612752': {'abbreviation': 'NYK', 'name': 'New York Knicks'},
        '1610612760': {'abbreviation': 'OKC', 'name': 'Oklahoma City Thunder'},
        '1610612753': {'abbreviation': 'ORL', 'name': 'Orlando Magic'},
        '1610612755': {'abbreviation': 'PHI', 'name': 'Philadelphia 76ers'},
        '1610612756': {'abbreviation': 'PHX', 'name': 'Phoenix Suns'},
        '1610612757': {'abbreviation': 'POR', 'name': 'Portland Trail Blazers'},
        '1610612758': {'abbreviation': 'SAC', 'name': 'Sacramento Kings'},
        '1610612759': {'abbreviation': 'SAS', 'name': 'San Antonio Spurs'},
        '1610612761': {'abbreviation': 'TOR', 'name': 'Toronto Raptors'},
        '1610612762': {'abbreviation': 'UTA', 'name': 'Utah Jazz'},
        '1610612764': {'abbreviation': 'WAS', 'name': 'Washington Wizards'}
    }

    # Test with recent date
test_date = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y')
games_df = extractor.get_games_for_date(test_date)

print(f"Found {len(games_df)} games for {test_date}")
if not games_df.empty:
    print(games_df.head())

### 2.2 Social Media Simulation (VS Code Development)

**Create data_generators/social_simulator.py:**

```python
"""
Social Media Data Simulation
Generates realistic Twitter-like social media data for NBA games
"""

import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
from textblob import TextBlob
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class SocialMediaSimulator:
    """
    Simulates realistic social media engagement for NBA games
    
    Features:
    - Template-based tweet generation
    - Realistic sentiment distributions
    - Engagement metrics based on follower counts
    - Time-based posting patterns
    - Star player mentions
    """
    
    def __init__(self):
        # Positive sentiment templates
        self.positive_templates = [
            "{player} is absolutely dominating tonight! #{team} #NBA #Basketball",
            "What a game! {team1} vs {team2} is living up to the hype! #NBA #Playoffs",
            "This {team} team chemistry is unreal! Love watching them play #NBA",
            "{player} with another clutch performance! #{team} #NBAPlayoffs",
            "Best game of the season! {team1} and {team2} going at it! #NBA",
            "Incredible defense by {team}! This is why I love basketball #NBA",
            "{player} is in the zone tonight! Can't miss! #{team} #Basketball",
            "The energy at this {team1} vs {team2} game is electric! #NBA #LiveBasketball",
            "Watching {player} play is pure art! #{team} #GOAT #NBA",
            "This {team} run is absolutely insane! Championship vibes! #NBA"
        ]
        
        # Negative sentiment templates  
        self.negative_templates = [
            "Terrible officiating in this {team1} vs {team2} game #NBA #Refs",
            "{team} looking flat tonight. Need to step it up #NBA #Disappointed",
            "This game is hard to watch. {team1} vs {team2} #NBA #Boring",
            "{player} having an off night. Happens to everyone #NBA #Sports",
            "Disappointing performance from {team} tonight #NBA #Basketball",
            "Too many turnovers by {team}. Clean it up! #NBA #Frustrated",
            "The refs are ruining this {team1} vs {team2} game #NBA #BadCalls",
            "Injuries killing the flow of this game #NBA #Unlucky",
            "{team} needs to make some changes. This isn't working #NBA",
            "Another blowout. Was hoping for a competitive game #NBA #Boring"
        ]
        
        # Neutral sentiment templates
        self.neutral_templates = [
            "Watching {team1} vs {team2} tonight #NBA #Basketball #Sports",
            "{player} playing solid basketball for {team} #NBA #Hoops",
            "Halftime: {team1} vs {team2}. Should be a good second half #NBA",
            "Game update: {team1} and {team2} battling it out #NBA #LiveScore",
            "{team} making some interesting lineup changes #NBA #Strategy",
            "Checking in on the {team1} vs {team2} game #NBA #Basketball",
            "Third quarter action between {team1} and {team2} #NBA #Q3",
            "{player} has {stats} points so far tonight #{team} #NBA",
            "Timeout called in the {team1} vs {team2} game #NBA #Strategy",
            "Close game between {team1} and {team2} going into the fourth #NBA"
        ]
        
        # Star players for realistic mentions
        self.star_players = {
            'LAL': ['LeBron James', 'Anthony Davis', 'Russell Westbrook'],
            'GSW': ['Stephen Curry', 'Klay Thompson', 'Draymond Green'],
            'BOS': ['Jayson Tatum', 'Jaylen Brown', 'Marcus Smart'],
            'MIL': ['Giannis Antetokounmpo', 'Khris Middleton', 'Jrue Holiday'],
            'PHX': ['Devin Booker', 'Kevin Durant', 'Chris Paul'],
            'BRK': ['Mikal Bridges', 'Cameron Johnson', 'Nic Claxton'],
            'MIA': ['Jimmy Butler', 'Bam Adebayo', 'Tyler Herro'],
            'PHI': ['Joel Embiid', 'James Harden', 'Tyrese Maxey'],
            'DEN': ['Nikola Jokic', 'Jamal Murray', 'Michael Porter Jr'],
            'MEM': ['Ja Morant', 'Jaren Jackson Jr', 'Desmond Bane'],
            'DAL': ['Luka Doncic', 'Kyrie Irving', 'Christian Wood'],
            'NYK': ['Julius Randle', 'Jalen Brunson', 'RJ Barrett'],
            'ATL': ['Trae Young', 'Dejounte Murray', 'John Collins'],
            'CHI': ['DeMar DeRozan', 'Zach LaVine', 'Nikola Vucevic'],
            'CLE': ['Donovan Mitchell', 'Darius Garland', 'Jarrett Allen']
        }
        
        # Hashtag patterns
        self.team_hashtags = {
            'LAL': ['#Lakers', '#LakeShow', '#LakeNation'],
            'GSW': ['#Warriors', '#DubNation', '#StrengthInNumbers'],
            'BOS': ['#Celtics', '#BleedGreen', '#CelticsPride'],
            'MIL': ['#Bucks', '#FearTheDeer', '#BucksNation'],
            'MIA': ['#Heat', '#HeatNation', '#CultureBuilt']
        }
    
    def generate_game_tweets(self, home_team: str, away_team: str, 
                           game_date: datetime.date, num_tweets: int = 500) -> pd.DataFrame:
        """
        Generate realistic tweets for a specific NBA game
        
        Args:
            home_team: Home team abbreviation
            away_team: Away team abbreviation  
            game_date: Date of the game
            num_tweets: Number of tweets to generate
            
        Returns:
            DataFrame with realistic tweet data
        """
        logger.info(f"Generating {num_tweets} tweets for {home_team} vs {away_team}")
        
        tweets = []
        
        # Get star players for both teams
        home_players = self.star_players.get(home_team, ['Star Player'])
        away_players = self.star_players.get(away_team, ['Star Player']) 
        all_players = home_players + away_players
        
        # Generate tweets throughout the game timeframe
        game_start = datetime.combine(game_date, datetime.min.time()) + timedelta(hours=19)  # 7 PM start
        game_end = game_start + timedelta(hours=3)  # 3-hour game window
        
        for i in range(num_tweets):
            # Choose sentiment distribution (realistic proportions)
            sentiment_choice = random.choices(
                ['positive', 'negative', 'neutral'], 
                weights=[25, 15, 60]  # 60% neutral, 25% positive, 15% negative
            )[0]
            
            # Select template and calculate sentiment score
            if sentiment_choice == 'positive':
                template = random.choice(self.positive_templates)
                sentiment_score = random.uniform(0.1, 0.8)
            elif sentiment_choice == 'negative':
                template = random.choice(self.negative_templates)
                sentiment_score = random.uniform(-0.8, -0.1)
            else:
                template = random.choice(self.neutral_templates)
                sentiment_score = random.uniform(-0.1, 0.1)
            
            # Fill in template with game-specific information
            selected_player = random.choice(all_players)
            stats = f"{random.randint(15, 45)}"  # Point total for realism
            
            text = template.format(
                team1=home_team,
                team2=away_team,
                team=random.choice([home_team, away_team]),
                player=selected_player,
                stats=stats
            )
            
            # Generate realistic engagement metrics
            followers = self._generate_follower_count()
            retweets = self._generate_retweets(followers, sentiment_score)
            favorites = self._generate_favorites(followers, retweets, sentiment_score)
            
            # Generate realistic timestamp during game
            tweet_time = self._generate_tweet_time(game_start, game_end, i, num_tweets)
            
            tweets.append({
                'tweet_id': self._generate_tweet_id(),
                'game_hashtag': f"{home_team}vs{away_team}",
                'text': text,
                'created_at': tweet_time,
                'user_followers': followers,
                'retweet_count': retweets,
                'favorite_count': favorites,
                'sentiment_polarity': round(sentiment_score, 2),
                'sentiment_subjectivity': round(random.uniform(0.1, 0.9), 2)
            })
        
        logger.info(f"Generated {len(tweets)} tweets successfully")
        return pd.DataFrame(tweets)
    
    def _generate_follower_count(self) -> int:
        """Generate realistic follower count distribution"""
        # Log-normal distribution for realistic social media following
        return max(10, int(np.random.lognormal(mean=7, sigma=2)))
    
    def _generate_retweets(self, followers: int, sentiment: float) -> int:
        """Generate retweets based on followers and sentiment"""
        base_rate = min(followers // 100, 50)  # Max 50 retweets
        sentiment_boost = max(0, sentiment * 2)  # Positive sentiment gets more retweets
        return max(0, int(random.gamma(2, base_rate + sentiment_boost)))
    
    def _generate_favorites(self, followers: int, retweets: int, sentiment: float) -> int:
        """Generate favorites (typically more than retweets)"""
        base_favorites = retweets * random.uniform(2, 5)
        follower_boost = min(followers // 50, 100)
        return max(retweets, int(base_favorites + follower_boost))
    
    def _generate_tweet_time(self, start: datetime, end: datetime, 
                           index: int, total: int) -> datetime:
        """Generate realistic tweet timing during game"""
        # More tweets during exciting moments (end of quarters, close game)
        game_duration = (end - start).total_seconds()
        
        # Create excitement spikes at quarter ends
        progress = index / total
        quarter_positions = [0.25, 0.5, 0.75, 1.0]  # End of each quarter
        
        # Add excitement around quarter ends
        excitement_multiplier = 1.0
        for qp in quarter_positions:
            if abs(progress - qp) < 0.05:  # Within 5% of quarter end
                excitement_multiplier = 2.0
                break
        
        # Distribute tweets with excitement weighting
        base_time = start + timedelta(seconds=progress * game_duration)
        jitter = timedelta(minutes=random.randint(-15, 15))
        
        return base_time + jitter
    
    def _generate_tweet_id(self) -> int:
        """Generate realistic Twitter-style ID"""
        return random.randint(1000000000000000000, 9999999999999999999)

# VS Code testing script
if __name__ == "__main__":
    # Test the social media simulator
    simulator = SocialMediaSimulator()
    
    # Generate sample tweets
    test_date = datetime.now().date()
    tweets_df = simulator.generate_game_tweets('LAL', 'GSW', test_date, 100)
    
    print(f"Generated {len(tweets_df)} tweets")
    print("\nSample tweets:")
    print(tweets_df[['text', 'sentiment_polarity', 'retweet_count']].head())
    
    print(f"\nSentiment distribution:")
    print(f"Positive: {len(tweets_df[tweets_df['sentiment_polarity'] > 0.1])}")
    print(f"Negative: {len(tweets_df[tweets_df['sentiment_polarity'] < -0.1])}")
    print(f"Neutral: {len(tweets_df[abs(tweets_df['sentiment_polarity']) <= 0.1])}")
