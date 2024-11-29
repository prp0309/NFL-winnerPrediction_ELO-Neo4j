import pandas as pd
import numpy as np

def calculate_current_season_averages(post_priori: pd.DataFrame, game_id: int, current_season: int) -> pd.DataFrame:
    """Calculate averages for the current season up to the given game.
    Args:
        post_priori (pd.DataFrame): DataFrame containing post-priori data.
        game_id (int): Game ID.
        current_season (int): Current season.
        
    Returns:
        pd.DataFrame: DataFrame with averages for the current season up to the given game.
        
    """
    game_date = post_priori[post_priori['game_id'] == game_id]['game_date'].iloc[0]
    current_season_data = post_priori[(post_priori['game_date'].dt.year == current_season) & (post_priori['game_date'] < game_date)]
    
    home_team = post_priori[post_priori['game_id'] == game_id]['home_team'].iloc[0]
    away_team = post_priori[post_priori['game_id'] == game_id]['away_team'].iloc[0]
    
    home_avg = current_season_data[current_season_data['home_team'] == home_team].mean()
    away_avg = current_season_data[current_season_data['away_team'] == away_team].mean()
    
    averages = pd.concat([home_avg.add_prefix('home_'), away_avg.add_prefix('away_')])
    averages['game_id'] = game_id
    
    return averages.to_frame().T

def calculate_mutual_game_averages(post_priori: pd.DataFrame, game_id: int) -> pd.DataFrame:
    """Calculate averages for mutual games played between teams.
    Args: 
        post_priori (pd.DataFrame): DataFrame containing post-priori data.
        game_id (int): Game ID.
        
    Returns:
        pd.DataFrame: DataFrame with averages for mutual games played between teams.
    """
    game = post_priori[post_priori['game_id'] == game_id].iloc[0]
    mutual_games = post_priori[
        ((post_priori['home_team'] == game['home_team']) & (post_priori['away_team'] == game['away_team'])) |
        ((post_priori['home_team'] == game['away_team']) & (post_priori['away_team'] == game['home_team']))
    ]
    mutual_games = mutual_games[mutual_games['game_date'] < game['game_date']]
    
    if len(mutual_games) == 0:
        return pd.DataFrame()
    
    averages = mutual_games.mean()
    averages['game_id'] = game_id
    
    return averages.to_frame().T

def calculate_last_n_games_averages(post_priori: pd.DataFrame, game_id: int, n: int) -> pd.DataFrame:
    """Calculate averages of the last n games for each team.
    Args:
        post_priori (pd.DataFrame): DataFrame containing post-priori data.
        game_id (int): Game ID.
        n (int): Number of games to consider.
        
    Returns:
        pd.DataFrame: DataFrame with averages of the last n games for each team.
    """
    game = post_priori[post_priori['game_id'] == game_id].iloc[0]
    home_team_games = post_priori[(post_priori['home_team'] == game['home_team']) | (post_priori['away_team'] == game['home_team'])]
    away_team_games = post_priori[(post_priori['home_team'] == game['away_team']) | (post_priori['away_team'] == game['away_team'])]
    
    home_team_games = home_team_games[home_team_games['game_date'] < game['game_date']].tail(n)
    away_team_games = away_team_games[away_team_games['game_date'] < game['game_date']].tail(n)
    
    if len(home_team_games) < n or len(away_team_games) < n:
        return pd.DataFrame()
    
    home_avg = home_team_games.mean()
    away_avg = away_team_games.mean()
    
    averages = pd.concat([home_avg.add_prefix('home_'), away_avg.add_prefix('away_')])
    averages['game_id'] = game_id
    
    return averages.to_frame().T

def calculate_last_m_mutual_games_averages(post_priori: pd.DataFrame, game_id: int, m: int) -> pd.DataFrame:
    """Calculate averages of the last m mutual games between teams.
    Args:
        post_priori (pd.DataFrame): DataFrame containing post-priori data.
        game_id (int): Game ID.
        m (int): Number of mutual games to consider.
        
    Returns:
        pd.DataFrame: DataFrame with averages of the last m mutual games between teams.
    """
    game = post_priori[post_priori['game_id'] == game_id].iloc[0]
    mutual_games = post_priori[
        ((post_priori['home_team'] == game['home_team']) & (post_priori['away_team'] == game['away_team'])) |
        ((post_priori['home_team'] == game['away_team']) & (post_priori['away_team'] == game['home_team']))
    ]
    mutual_games = mutual_games[mutual_games['game_date'] < game['game_date']].tail(m)
    
    if len(mutual_games) < m:
        return pd.DataFrame()
    
    averages = mutual_games.mean()
    averages['game_id'] = game_id
    
    return averages.to_frame().T

def calculate_averages_by_scenario(post_priori: pd.DataFrame) -> dict:
    """Calculate averages for all 13 scenarios for each game.
    Args:
        post_priori (pd.DataFrame): DataFrame containing post-priori data.
        
    Returns:
        dict: Dictionary containing DataFrames with averages for each scenario.
    """
    post_priori['game_date'] = pd.to_datetime(post_priori['game_date'])
    post_priori = post_priori.sort_values('game_date')
    
    scenarios = {
        'current_season': pd.DataFrame(),
        'mutual_games': pd.DataFrame(),
        'last_3_games': pd.DataFrame(),
        'last_5_games': pd.DataFrame(),
        'last_7_games': pd.DataFrame(),
        'last_8_games': pd.DataFrame(),
        'last_9_games': pd.DataFrame(),
        'last_10_games': pd.DataFrame(),
        'last_11_games': pd.DataFrame(),
        'last_2_mutual_games': pd.DataFrame(),
        'last_3_mutual_games': pd.DataFrame(),
        'last_5_mutual_games': pd.DataFrame(),
        'last_7_mutual_games': pd.DataFrame()
    }
    
    for game_id in post_priori['game_id']:
        current_season = post_priori[post_priori['game_id'] == game_id]['game_date'].dt.year.iloc[0]
        
        scenarios['current_season'] = pd.concat([scenarios['current_season'], calculate_current_season_averages(post_priori, game_id, current_season)])
        scenarios['mutual_games'] = pd.concat([scenarios['mutual_games'], calculate_mutual_game_averages(post_priori, game_id)])
        
        for n in [3, 5, 7, 8, 9, 10, 11]:
            scenarios[f'last_{n}_games'] = pd.concat([scenarios[f'last_{n}_games'], calculate_last_n_games_averages(post_priori, game_id, n)])
        
        for m in [2, 3, 5, 7]:
            scenarios[f'last_{m}_mutual_games'] = pd.concat([scenarios[f'last_{m}_mutual_games'], calculate_last_m_mutual_games_averages(post_priori, game_id, m)])
    
    # Fill missing values with 0
    for scenario in scenarios:
        scenarios[scenario] = scenarios[scenario].fillna(0)
    
    return scenarios

# Load post-priori data
post_priori = pd.read_csv('post_priori.csv')

# Calculate averages for all scenarios
all_averages = calculate_averages_by_scenario(post_priori)

# Save results to CSV files
for scenario, df in all_averages.items():
    df.to_csv(f'averages_{scenario}.csv', index=False)

print("Averages calculated and saved for all scenarios.")