"""
TV Ratings Simulation
Generates realistic TV viewership data for NBA games
"""
import pandas as pd
import random
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List  # Added Dict and List imports
import logging

logger = logging.getLogger(__name__)

class TVRatingsSimulator:
    """
    Simulates realistic TV ratings for NBA games based on:
    - Team popularity and market size
    - Day of week and time slot effects
    - Game competitiveness and star power
    - Seasonal trends and special events
    """
    
    def __init__(self):
        # Team popularity factors (based on market size, recent success, star power)
        self.team_popularity = {
            # Large market + superstar teams
            'LAL': 3.5, 'GSW': 3.2, 'BOS': 2.8, 'NYK': 2.6,
            # Strong fan bases
            'CHI': 2.4, 'MIA': 2.3, 'PHI': 2.2, 'DAL': 2.1,
            # Competitive teams
            'MIL': 2.0, 'PHX': 1.9, 'BRK': 1.8, 'DEN': 1.7,
            # Mid-market teams
            'ATL': 1.6, 'MEM': 1.5, 'MIN': 1.4, 'CLE': 1.4,
            'TOR': 1.4, 'UTA': 1.3, 'POR': 1.3, 'SAS': 1.3,
            # Smaller markets
            'WAS': 1.2, 'NOP': 1.1, 'SAC': 1.1, 'ORL': 1.1,
            'IND': 1.1, 'CHA': 1.0, 'DET': 1.0, 'OKC': 1.1,
            'HOU': 1.5, 'LAC': 1.7  # Market dependent
        }
        
        # Day of week multipliers (realistic TV viewing patterns)
        self.day_multipliers = {
            'Monday': 0.85,    # Low viewership
            'Tuesday': 0.90,   # Building up
            'Wednesday': 0.95, # Midweek
            'Thursday': 1.05,  # TNT night
            'Friday': 1.15,    # Weekend starts
            'Saturday': 1.25,  # Peak weekend
            'Sunday': 1.20     # Sunday night basketball
        }
        
        # Time slot effects
        self.time_slot_effects = {
            'early': 0.8,      # 6 PM ET games
            'prime': 1.4,      # 8 PM ET games
            'late': 0.9,       # 10:30 PM ET games
            'afternoon': 0.7   # Weekend afternoon games
        }
        
        # Monthly seasonal effects
        self.seasonal_multipliers = {
            10: 1.0,  # October - season start
            11: 1.1,  # November - building interest
            12: 1.2,  # December - holiday games
            1: 1.3,   # January - peak season
            2: 1.4,   # February - All-Star, trade deadline
            3: 1.5,   # March - playoff race
            4: 1.8,   # April - playoffs
            5: 2.0,   # May - conference finals
            6: 2.2    # June - NBA Finals
        }

    def generate_game_ratings(self, home_team: str, away_team: str, 
                            game_date: date) -> Dict:
        """
        Generate realistic TV ratings for a specific game
        
        Args:
            home_team: Home team abbreviation
            away_team: Away team abbreviation
            game_date: Date of the game
            
        Returns:
            Dictionary with comprehensive rating data
        """
        logger.info(f"Generating TV ratings for {home_team} vs {away_team} on {game_date}")
        
        # Base rating from team popularity
        home_popularity = self.team_popularity.get(home_team, 1.0)
        away_popularity = self.team_popularity.get(away_team, 1.0)
        
        # Combine team popularity (higher of the two has more weight)
        base_rating = (max(home_popularity, away_popularity) * 0.7 + 
                      min(home_popularity, away_popularity) * 0.3)
        
        # Day of week effect
        day_name = game_date.strftime('%A')
        day_multiplier = self.day_multipliers.get(day_name, 1.0)
        
        # Time slot determination (realistic distribution)
        time_slot = self._determine_time_slot(day_name)
        time_multiplier = self.time_slot_effects[time_slot]
        
        # Seasonal effect
        month = game_date.month
        seasonal_multiplier = self.seasonal_multipliers.get(month, 1.0)
        
        # Special game factors
        rivalry_boost = self._calculate_rivalry_boost(home_team, away_team)
        competitiveness_factor = random.uniform(0.8, 1.3)  # Game-specific excitement
        
        # Calculate final rating
        final_rating = (base_rating * day_multiplier * time_multiplier * 
                       seasonal_multiplier * rivalry_boost * competitiveness_factor)
        
        # Add realistic variance and bounds
        final_rating = max(0.3, min(8.0, final_rating + random.gauss(0, 0.15)))
        
        # Estimated viewers (millions) - rating * households * viewing factor
        estimated_viewers = final_rating * random.uniform(1.1, 1.4)
        
        # Demographic breakdowns (realistic distributions)
        demographics = self._generate_demographics(final_rating, home_team, away_team)
        
        return {
            'game_date': game_date,
            'home_team': home_team,
            'away_team': away_team,
            'tv_rating': round(final_rating, 2),
            'estimated_viewers': round(estimated_viewers, 2),
            'is_weekend': day_name in ['Saturday', 'Sunday'],
            'is_primetime': time_slot == 'prime',
            'time_slot': time_slot,
            'day_of_week': day_name,
            'rivalry_factor': round(rivalry_boost, 2),
            'home_team_popularity': home_popularity,
            'away_team_popularity': away_popularity,
            **demographics
        }
    
    def _determine_time_slot(self, day_name: str) -> str:
        """Determine realistic time slot based on day"""
        if day_name in ['Saturday', 'Sunday']:
            return random.choices(
                ['afternoon', 'prime', 'late'],
                weights=[40, 50, 10]
            )[0]
        else:
            return random.choices(
                ['early', 'prime', 'late'],
                weights=[20, 60, 20]
            )[0]
    
    def _calculate_rivalry_boost(self, home_team: str, away_team: str) -> float:
        """Calculate boost for rivalry games"""
        # Define major rivalries
        rivalries = {
            ('LAL', 'BOS'), ('BOS', 'LAL'),  # Historic rivalry
            ('LAL', 'GSW'), ('GSW', 'LAL'),  # California clash
            ('NYK', 'BRK'), ('BRK', 'NYK'),  # New York battle
            ('MIA', 'BOS'), ('BOS', 'MIA'),  # Eastern rivalry
            ('GSW', 'CLE'), ('CLE', 'GSW'),  # Finals rivalry
            ('CHI', 'DET'), ('DET', 'CHI'),  # Central division
            ('SAS', 'DAL'), ('DAL', 'SAS'),  # Texas rivalry
            ('LAC', 'LAL'), ('LAL', 'LAC'),  # Battle of LA
        }
        
        if (home_team, away_team) in rivalries:
            return random.uniform(1.3, 1.6)  # 30-60% boost for rivalries
        elif home_team == away_team:  # Same conference/division
            return random.uniform(1.1, 1.2)  # Small boost for familiarity
        else:
            return random.uniform(0.95, 1.05)  # Neutral matchup
    
    def _generate_demographics(self, rating: float, home_team: str, away_team: str) -> Dict:
        """Generate realistic demographic breakdowns"""
        # Age demographics (varies by team popularity and time slot)
        total_viewers = rating * 1.2 * 1000000  # Convert to absolute numbers
        
        return {
            'viewers_18_34': round(total_viewers * random.uniform(0.25, 0.35), 0),
            'viewers_35_54': round(total_viewers * random.uniform(0.35, 0.45), 0),
            'viewers_55_plus': round(total_viewers * random.uniform(0.20, 0.30), 0),
            'male_viewers_pct': round(random.uniform(60, 75), 1),
            'female_viewers_pct': round(random.uniform(25, 40), 1),
            'streaming_viewers_pct': round(random.uniform(15, 35), 1)  # Growing trend
        }
    
    def generate_season_ratings(self, start_date: date, end_date: date, 
                              games_per_day: int = 8) -> pd.DataFrame:
        """Generate ratings for an entire season period"""
        logger.info(f"Generating season ratings from {start_date} to {end_date}")
        
        all_ratings = []
        current_date = start_date
        teams = list(self.team_popularity.keys())
        
        while current_date <= end_date:
            # Generate random games for the day
            daily_games = min(games_per_day, len(teams) // 2)
            
            if daily_games > 0:
                # Randomly select teams for games
                available_teams = teams.copy()
                random.shuffle(available_teams)
                
                for i in range(0, daily_games * 2, 2):
                    if i + 1 < len(available_teams):
                        home_team = available_teams[i]
                        away_team = available_teams[i + 1]
                        
                        rating_data = self.generate_game_ratings(
                            home_team, away_team, current_date
                        )
                        all_ratings.append(rating_data)
            
            current_date += timedelta(days=1)
        
        return pd.DataFrame(all_ratings)


# VS Code testing script
if __name__ == "__main__":
    # Test the ratings simulator
    simulator = TVRatingsSimulator()
    
    # Generate sample ratings
    test_date = datetime.now().date()
    rating = simulator.generate_game_ratings('LAL', 'GSW', test_date)
    
    print("Sample TV Rating Data:")
    for key, value in rating.items():
        print(f"{key}: {value}")
    
    # Test seasonal generation
    start_date = test_date - timedelta(days=7)
    end_date = test_date
    season_ratings = simulator.generate_season_ratings(start_date, end_date, 4)
    
    print(f"\nGenerated {len(season_ratings)} game ratings")
    print(season_ratings[['home_team', 'away_team', 'tv_rating', 'estimated_viewers']].head())