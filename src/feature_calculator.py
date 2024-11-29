import pandas as pd
def calculate_post_priori(data: pd.DataFrame) -> pd.DataFrame:
    
    post_priori = pd.DataFrame()
    post_priori['game_id'] = data['game_id'].unique()
    post_priori = post_priori.merge(data[['game_id', 'game_date', 'home_team', 'away_team']].drop_duplicates(), on='game_id', how='left')
        
    #calculating post_priori_characteristics by calling the functions below and merging the dataframes
    scores = calculate_scores(data)
    conv_perc = calculate_conv_perc(data)
    turnovers = calculate_turnovers(data)
    downs = total_downs(data)
    penalties = penalties_and_yard_penalties_gained(data)
    fd_penalty = calculate_fd_due_to_penalty_gained(data)
    time_of_possession = calculate_time_of_possession(data)
    yards_gained = calculate_yards_gained(data)
    play_count = calculate_tot_play_count(data)
    score_last_2_minutes_q2 = calculate_score_last_2_minutes_q2(data)
    score_last_2_minutes_q4 = calculate_score_last_2_minutes_q4(data)
    offensive_metrics = calculate_offensive_metrics(data)
    defensive_metrics = calculate_defensive_metrics(data)
    
    post_priori = pd.merge(post_priori, scores, on='game_id')
    post_priori = pd.merge(post_priori, conv_perc, on='game_id')
    post_priori = pd.merge(post_priori, turnovers, on='game_id')
    post_priori = pd.merge(post_priori, downs, on='game_id')
    post_priori = pd.merge(post_priori, penalties, on='game_id')
    post_priori = pd.merge(post_priori, fd_penalty, on='game_id')
    post_priori = pd.merge(post_priori, time_of_possession, on='game_id')
    post_priori = pd.merge(post_priori, yards_gained, on='game_id')
    post_priori = pd.merge(post_priori, play_count, on='game_id')
    post_priori = pd.merge(post_priori, score_last_2_minutes_q2, on='game_id')
    post_priori = pd.merge(post_priori, score_last_2_minutes_q4, on='game_id')
    post_priori = pd.merge(post_priori, offensive_metrics, on='game_id')
    post_priori = pd.merge(post_priori, defensive_metrics, on='game_id')
    
    post_priori.to_csv('data/processed/post_priori.csv', index=False)
    return post_priori

def calculate_scores(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate scores for each quarter and total scores for each game.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with scores for each quarter and total scores for each game.
    '''
    scores = data.groupby(['game_id']).agg({'total_home_score': 'max', 'total_away_score': 'max'}).reset_index()
    scores['point_diff'] = scores['total_home_score'] - scores['total_away_score']
    
    #scores for each quarter
    home_qtr_score = pd.Series([0] * len(scores))
    away_qtr_score = pd.Series([0] * len(scores))
    
    for qtr in range(1, 6):
        qtr_scores = data[data['qtr'] == qtr].groupby(['game_id'])[['total_home_score', 'total_away_score']].max().reset_index()
        scores['score_q' + str(qtr) + '_home'] = qtr_scores['total_home_score']
        scores['score_q' + str(qtr) + '_away'] = qtr_scores['total_away_score']
        
        scores['score_q' + str(qtr) + '_allow_home'] = scores['score_q' + str(qtr) + '_away'] - away_qtr_score
        scores['score_q' + str(qtr) + '_allow_away'] = scores['score_q' + str(qtr) + '_home'] - home_qtr_score
        
        home_qtr_score = scores['score_q' + str(qtr) + '_home']
        away_qtr_score = scores['score_q' + str(qtr) + '_away']
    
    scores['result'] = scores.apply(lambda row: 'home_win' if row['total_home_score'] > row['total_away_score'] else ('away_win' if row['total_home_score'] < row['total_away_score'] else 'tie'), axis=1)
    
    return scores

def calculate_conv_perc(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate conversion percentage for each down.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with conversion percentage for each down.
    '''
    home_conv_perc = data[data['posteam_type'] == 'home'].groupby(['game_id', 'posteam_type']).agg({'third_down_converted': 'sum', 'third_down_failed': 'sum', 'fourth_down_converted': 'sum', 'fourth_down_failed':'sum'}).reset_index()
    away_conv_perc = data[data['posteam_type'] == 'away'].groupby(['game_id', 'posteam_type']).agg({'third_down_converted': 'sum', 'third_down_failed': 'sum', 'fourth_down_converted': 'sum', 'fourth_down_failed':'sum'}).reset_index()
    
    home_conv_perc['third_down_conv_perc'] = (home_conv_perc['third_down_converted'] / (home_conv_perc['third_down_converted'] + home_conv_perc['third_down_failed'])) * 100
    home_conv_perc['fourth_down_conv_perc'] = (home_conv_perc['fourth_down_converted'] / (home_conv_perc['fourth_down_converted'] + home_conv_perc['fourth_down_failed'])) * 100
    
    away_conv_perc['third_down_conv_perc'] = (away_conv_perc['third_down_converted'] / (away_conv_perc['third_down_converted'] + away_conv_perc['third_down_failed'])) * 100
    away_conv_perc['fourth_down_conv_perc'] = (away_conv_perc['fourth_down_converted'] / (away_conv_perc['fourth_down_converted'] + away_conv_perc['fourth_down_failed'])) * 100
    
    home_conv_perc['third_down_conv_perc_allow'] = away_conv_perc['third_down_conv_perc']
    away_conv_perc['third_down_conv_perc_allow'] = home_conv_perc['third_down_conv_perc']
    
    home_conv_perc.drop(columns=['third_down_converted', 'third_down_failed', 'fourth_down_converted', 'fourth_down_failed'], inplace=True)
    away_conv_perc.drop(columns=['third_down_converted', 'third_down_failed', 'fourth_down_converted', 'fourth_down_failed'], inplace=True)
    
    return pd.merge(home_conv_perc, away_conv_perc, on='game_id', suffixes=('_home', '_away'))

def calculate_turnovers(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate turnovers for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with turnovers for each team.
    '''
    home_turnovers = data[data['posteam_type'] == 'home'].groupby(['game_id', 'posteam_type']).agg({'fumble': 'sum', 'interception': 'sum'}).reset_index()
    away_turnovers = data[data['posteam_type'] == 'away'].groupby(['game_id', 'posteam_type']).agg({'fumble': 'sum', 'interception': 'sum'}).reset_index()
    
    home_turnovers['total_turnovers'] = home_turnovers['fumble'] + home_turnovers['interception']
    away_turnovers['total_turnovers'] = away_turnovers['fumble'] + away_turnovers['interception']
    
    home_turnovers['total_turnovers_allow'] = away_turnovers['total_turnovers']
    away_turnovers['total_turnovers_allow'] = home_turnovers['total_turnovers']
    
    home_turnovers.drop(columns=['fumble', 'interception'], inplace=True)
    away_turnovers.drop(columns=['fumble', 'interception'], inplace=True)
    
    return pd.merge(home_turnovers, away_turnovers, on='game_id', suffixes=('_home', '_away'))

def total_downs(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate total downs for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with total downs for each team.
    '''
    home_downs = data[data['posteam_type'] == 'home'].groupby(['game_id'])[['first_down_rush', 'first_down_pass', 'first_down_penalty']].sum().sum(axis=1).reset_index()
    away_downs = data[data['posteam_type'] == 'away'].groupby(['game_id'])[['first_down_rush', 'first_down_pass', 'first_down_penalty']].sum().sum(axis=1).reset_index()
    
    home_downs.columns = ['game_id', 'total_first_downs']
    away_downs.columns = ['game_id', 'total_first_downs']
    
    home_downs['tot_pass_first_downs'] = data[data['posteam_type'] == 'home'].groupby(['game_id'])['first_down_pass'].sum().reset_index()['first_down_pass']
    away_downs['tot_pass_first_downs'] = data[data['posteam_type'] == 'away'].groupby(['game_id'])['first_down_pass'].sum().reset_index()['first_down_pass']
    
    home_downs['tot_rush_first_downs'] = data[data['posteam_type'] == 'home'].groupby(['game_id'])['first_down_rush'].sum().reset_index()['first_down_rush']
    away_downs['tot_rush_first_downs'] = data[data['posteam_type'] == 'away'].groupby(['game_id'])['first_down_rush'].sum().reset_index()['first_down_rush']
    
    
    
    return pd.merge(home_downs, away_downs, on='game_id', suffixes=('_home', '_away'))

def penalties_and_yard_penalties_gained(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate total penalties and yards gained for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with total penalties and yards gained for each team.
    '''
    home_penalties = data[(data['penalty'] == 1) & (data['penalty_team'] == data['home_team'])].groupby(['game_id']).agg({'penalty': 'sum', 'penalty_yards': 'sum'}).reset_index()
    away_penalties = data[(data['penalty'] == 1) & (data['penalty_team'] == data['away_team'])].groupby(['game_id']).agg({'penalty': 'sum', 'penalty_yards': 'sum'}).reset_index()
    
    home_penalties.columns = ['game_id', 'num_penalties_gained', 'yards_penalties_gained']
    away_penalties.columns = ['game_id', 'num_penalties_gained', 'yards_penalties_gained']
    
    home_penalties['num_penalties_allowed'] = away_penalties['num_penalties_gained']
    home_penalties['yards_penalties_allowed'] = away_penalties['yards_penalties_gained']
    
    away_penalties['num_penalties_allowed'] = home_penalties['num_penalties_gained']
    away_penalties['yards_penalties_allowed'] = home_penalties['yards_penalties_gained']
    
    return pd.merge(home_penalties, away_penalties, on='game_id', suffixes=('_home', '_away'))

def calculate_fd_due_to_penalty_gained(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate first downs due to penalties gained for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with first downs due to penalties gained for each team.
    '''
    
    home_fd_penalty = data[data['posteam_type'] == 'home'].groupby(['game_id']).agg({'first_down_penalty': 'sum'}).reset_index()
    away_fd_penalty = data[data['posteam_type'] == 'away'].groupby(['game_id']).agg({'first_down_penalty': 'sum'}).reset_index()

    home_fd_penalty.columns = ['game_id', 'fd_due_to_penalty_gained']
    away_fd_penalty.columns = ['game_id', 'fd_due_to_penalty_gained']

    home_fd_penalty['fd_due_to_penalty_allow'] = away_fd_penalty['fd_due_to_penalty_gained']
    away_fd_penalty['fd_due_to_penalty_allow'] = home_fd_penalty['fd_due_to_penalty_gained']

    
    return pd.merge(home_fd_penalty, away_fd_penalty, on='game_id', suffixes=('_home', '_away'))

def calculate_time_of_possession(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate time of possession for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with time of possession for each team.
    '''
    data = data.sort_values(by=['game_id', 'play_id'])
    
    data['time_elapsed'] = data.groupby(['game_id', 'posteam_type'])['game_seconds_remaining'].diff(-1).fillna(0)
    
    home_time_of_possession = data[data['posteam_type'] == 'home'].groupby(['game_id'])['time_elapsed'].sum().reset_index()
    home_time_of_possession.columns = ['game_id', 'time_of_possession']
    
    away_time_of_possession = data[data['posteam_type'] == 'away'].groupby(['game_id'])['time_elapsed'].sum().reset_index()
    away_time_of_possession.columns = ['game_id', 'time_of_possession']
    
    return pd.merge(home_time_of_possession, away_time_of_possession, on='game_id', suffixes=('_home', '_away'))


def calculate_yards_gained(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate total yards gained for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with total yards gained for each team.
    '''
    home_yards_gained = data[data['posteam_type'] == 'home'].groupby(['game_id'])['yards_gained'].sum().reset_index()
    away_yards_gained = data[data['posteam_type'] == 'away'].groupby(['game_id'])['yards_gained'].sum().reset_index()
    
    home_yards_gained.columns = ['game_id', 'yards_gained_home']
    away_yards_gained.columns = ['game_id', 'yards_gained_away']
    
    return pd.merge(home_yards_gained, away_yards_gained, on='game_id')

def calculate_tot_play_count(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate total number of plays for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with total number of plays for each team.
    '''
    home_play_count = data[data['posteam_type'] == 'home'].groupby(['game_id'])['play_id'].count().reset_index()
    away_play_count = data[data['posteam_type'] == 'away'].groupby(['game_id'])['play_id'].count().reset_index()
    
    home_play_count.columns = ['game_id', 'total_plays_home']
    away_play_count.columns = ['game_id', 'total_plays_away']
    
    return pd.merge(home_play_count, away_play_count, on='game_id')

# def calculate_score_last_2_minutes_q2(data: pd.DataFrame) -> pd.DataFrame:
#     '''
#     Calculate scores in the last two minutes of the second quarter for each team.
    
#     Args:
#         data (pd.DataFrame): DataFrame containing play-by-play data.
        
#     Returns:
#         pd.DataFrame: DataFrame with scores in the last two minutes of the second quarter for each team.
#     '''
    
#     # Filter data to last 2 minutes of 2nd quarter
#     last_2_min_q2 = data[(data['game_seconds_remaining'] <= 1920) & (data['game_seconds_remaining'] > 1800) & (data['qtr'] == 2)]

#     # Calculate score for home team in last 2 minutes of Q2
#     home_score_last_2_min_q2 = last_2_min_q2[last_2_min_q2['posteam_type'] == 'home'].groupby(['game_id'])['total_home_score'].max().reset_index()
#     home_score_last_2_min_q2.columns = ['game_id', 'score_last_2_min_q2_home']

#     # Calculate score for away team in last 2 minutes of Q2
#     away_score_last_2_min_q2 = last_2_min_q2[last_2_min_q2['posteam_type'] == 'away'].groupby(['game_id'])['total_away_score'].max().reset_index()
#     away_score_last_2_min_q2.columns = ['game_id', 'score_last_2_min_q2_away']
    
#     return pd.merge(home_score_last_2_min_q2, away_score_last_2_min_q2, on='game_id')

# def calculate_score_last_2_minutes_q4(data: pd.DataFrame) -> pd.DataFrame:
#     '''
#     Calculate scores in the last two minutes of the second quarter for each team.
    
#     Args:
#         data (pd.DataFrame): DataFrame containing play-by-play data.
        
#     Returns:
#         pd.DataFrame: DataFrame with scores in the last two minutes of the second quarter for each team.
#     '''
#     data = data[(data['qtr'] == 4) & (data['game_seconds_remaining'] <= 120)]
    
#     home_score_last_2_minutes_q4 = data[data['posteam_type'] == 'home'].groupby(['game_id'])['total_home_score'].max().reset_index()
#     away_score_last_2_minutes_q4 = data[data['posteam_type'] == 'away'].groupby(['game_id'])['total_away_score'].max().reset_index()
    
#     home_score_last_2_minutes_q4.columns = ['game_id', 'score_last_2_minutes_q4_home']
#     away_score_last_2_minutes_q4.columns = ['game_id', 'score_last_2_minutes_q4_away']
    
#     return pd.merge(home_score_last_2_minutes_q4, away_score_last_2_minutes_q4, on='game_id')

def calculate_score_last_2_minutes_q2(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate scores in the last two minutes of the second quarter for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with scores in the last two minutes of the second quarter for each team.
    """
    
    # Filter data to last 2 minutes of 2nd quarter
    last_2_min_q2 = data[(data['game_seconds_remaining'] <= 1920) & (data['game_seconds_remaining'] > 1800) & (data['qtr'] == 2)]

    # Calculate score for home team in last 2 minutes of Q2
    home_score_last_2_min_q2 = last_2_min_q2.groupby(['game_id', 'home_team'])['total_home_score'].max().reset_index()
    home_score_last_2_min_q2.columns = ['game_id', 'home_team', 'score_last_2_min_q2_home']

    # Calculate score for away team in last 2 minutes of Q2
    away_score_last_2_min_q2 = last_2_min_q2.groupby(['game_id', 'away_team'])['total_away_score'].max().reset_index()
    away_score_last_2_min_q2.columns = ['game_id', 'away_team', 'score_last_2_min_q2_away']
    
    # Merge home and away scores
    scores_last_2_min_q2 = pd.merge(home_score_last_2_min_q2, away_score_last_2_min_q2, on='game_id', how='outer')
    
    # Fill NaN values with 0 (for games where there were no plays in the last 2 minutes of Q2)
    scores_last_2_min_q2 = scores_last_2_min_q2.fillna(0)
    
    return scores_last_2_min_q2

def calculate_score_last_2_minutes_q4(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate scores in the last two minutes of the fourth quarter for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with scores in the last two minutes of the fourth quarter for each team.
    """
    # Filter data to last 2 minutes of 4th quarter
    last_2_min_q4 = data[(data['game_seconds_remaining'] <= 120) & (data['qtr'] == 4)]
    
    # Calculate score for home team in last 2 minutes of Q4
    home_score_last_2_minutes_q4 = last_2_min_q4.groupby(['game_id', 'home_team'])['total_home_score'].max().reset_index()
    home_score_last_2_minutes_q4.columns = ['game_id', 'home_team', 'score_last_2_minutes_q4_home']
    
    # Calculate score for away team in last 2 minutes of Q4
    away_score_last_2_minutes_q4 = last_2_min_q4.groupby(['game_id', 'away_team'])['total_away_score'].max().reset_index()
    away_score_last_2_minutes_q4.columns = ['game_id', 'away_team', 'score_last_2_minutes_q4_away']
    
    # Merge home and away scores
    scores_last_2_min_q4 = pd.merge(home_score_last_2_minutes_q4, away_score_last_2_minutes_q4, on='game_id', how='outer')
    
    # Fill NaN values with 0 (for games where there were no plays in the last 2 minutes of Q4)
    scores_last_2_min_q4 = scores_last_2_min_q4.fillna(0)
    
    return scores_last_2_min_q4
def calculate_offensive_metrics(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate offensive metrics for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with offensive metrics for each team.
    '''
    
    # Create a DataFrame with unique game_id values and respective home and away teams
    game_ids = data[['game_id', 'home_team', 'away_team']].drop_duplicates()
    
    
    # Field Goals Made
    home_fg = data[(data['posteam_type'] == 'home') & (data['field_goal_result'] == 'made')].groupby(['game_id'])['field_goal_result'].count().reset_index()
    away_fg = data[(data['posteam_type'] == 'away') & (data['field_goal_result'] == 'made')].groupby(['game_id'])['field_goal_result'].count().reset_index()
    
    home_fg.columns = ['game_id', 'off_kicking_fg_made_home']
    away_fg.columns = ['game_id', 'off_kicking_fg_made_away']
    
    # Field Goals Missed
    home_fg_missed = data[(data['posteam_type'] == 'home') & (data['field_goal_result'] == 'missed')].groupby(['game_id'])['field_goal_result'].count().reset_index()
    away_fg_missed = data[(data['posteam_type'] == 'away') & (data['field_goal_result'] == 'missed')].groupby(['game_id'])['field_goal_result'].count().reset_index()
    
    home_fg_missed.columns = ['game_id', 'off_kicking_fg_missed_home']
    away_fg_missed.columns = ['game_id', 'off_kicking_fg_missed_away']
    
    # Total Kickoff yards
    home_kickoff_yds = data[(data['posteam_type'] == 'home') & (data['play_type'] == 'kickoff')].groupby(['game_id'])['kick_distance'].sum().reset_index()
    away_kickoff_yds = data[(data['posteam_type'] == 'away') & (data['play_type'] == 'kickoff')].groupby(['game_id'])['kick_distance'].sum().reset_index()
    
    home_kickoff_yds.columns = ['game_id', 'off_tot_kickoff_yards_home']
    away_kickoff_yds.columns = ['game_id', 'off_tot_kickoff_yards_away']
    
    # Total Kick Return Yards
    home_kickret_yds = data[(data['posteam_type'] == 'home') & (data['play_type'] == 'kickoff')].groupby(['game_id'])['return_yards'].sum().reset_index()
    away_kickret_yds = data[(data['posteam_type'] == 'away') & (data['play_type'] == 'kickoff')].groupby(['game_id'])['return_yards'].sum().reset_index()
    
    home_kickret_yds.columns = ['game_id', 'off_tot_kickret_yards_home']
    away_kickret_yds.columns = ['game_id', 'off_tot_kickret_yards_away']
    
    # Total Punt Return Yards
    home_puntret_yds = data[(data['posteam_type'] == 'home') & (data['play_type'] == 'punt')].groupby(['game_id'])['return_yards'].sum().reset_index()
    away_puntret_yds = data[(data['posteam_type'] == 'away') & (data['play_type'] == 'punt')].groupby(['game_id'])['return_yards'].sum().reset_index()
    
    home_puntret_yds.columns = ['game_id', 'off_tot_puntret_yards_home']
    away_puntret_yds.columns = ['game_id', 'off_tot_puntret_yards_away']
    
    # Total Passing attempts
    home_pass_attempts = data[(data['posteam_type'] == 'home') & (data['play_type'] == 'pass')].groupby(['game_id']).size().reset_index()
    away_pass_attempts = data[(data['posteam_type'] == 'away') & (data['play_type'] == 'pass')].groupby(['game_id']).size().reset_index()
    
    home_pass_attempts.columns = ['game_id', 'off_tot_pass_attempts_home']
    away_pass_attempts.columns = ['game_id', 'off_tot_pass_attempts_away']
    
    # Total Passing completions
    home_passing_cmp = data[(data['posteam_type'] == 'home') & (data['complete_pass'] == 1)].groupby(['game_id']).size().reset_index()
    away_passing_cmp = data[(data['posteam_type'] == 'away') & (data['complete_pass'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_passing_cmp.columns = ['game_id', 'off_tot_pass_cmp_home']
    away_passing_cmp.columns = ['game_id', 'off_tot_pass_cmp_away']
    
    # Total Passing interceptions
    home_passing_int = data[(data['posteam_type'] == 'home') & (data['interception'] == 1)].groupby(['game_id']).size().reset_index()
    away_passing_int = data[(data['posteam_type'] == 'away') & (data['interception'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_passing_int.columns = ['game_id', 'off_tot_pass_int_home']
    away_passing_int.columns = ['game_id', 'off_tot_pass_int_away']
    
    # Sacks
    home_passing_sacks = data[(data['posteam_type'] == 'home') & (data['sack'] == 1)].groupby(['game_id']).size().reset_index()
    away_passing_sacks = data[(data['posteam_type'] == 'away') & (data['sack'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_passing_sacks.columns = ['game_id', 'off_tot_pass_sacks_home']
    away_passing_sacks.columns = ['game_id', 'off_tot_pass_sacks_away']
    
    # Passing Touchdowns
    home_passing_tds = data[(data['posteam_type'] == 'home') & (data['pass_touchdown'] == 1)].groupby(['game_id']).size().reset_index()
    away_passing_tds = data[(data['posteam_type'] == 'away') & (data['pass_touchdown'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_passing_tds.columns = ['game_id', 'off_tot_pass_tds_home']
    away_passing_tds.columns = ['game_id', 'off_tot_pass_tds_away']
    
    # Passing Yards
    home_passing_yds = data[(data['posteam_type'] == 'home') & (data['play_type'] == 'pass')].groupby(['game_id'])['yards_gained'].sum().reset_index()
    away_passing_yds = data[(data['posteam_type'] == 'away') & (data['play_type'] == 'pass')].groupby(['game_id'])['yards_gained'].sum().reset_index()
    
    home_passing_yds.columns = ['game_id', 'off_tot_pass_yds_home']
    away_passing_yds.columns = ['game_id', 'off_tot_pass_yds_away']
    
    # Passing Completion Percentage
    home_pass_cmp_perc = data[(data['posteam_type'] == 'home') & (data['play_type'] == 'pass')].groupby(['game_id']).agg({'complete_pass': 'sum', 'play_type': 'count'}).reset_index()
    home_pass_cmp_perc['off_pass_cmp_perc_home'] = (home_pass_cmp_perc['complete_pass'] / home_pass_cmp_perc['play_type']) * 100
    
    away_pass_cmp_perc = data[(data['posteam_type'] == 'away') & (data['play_type'] == 'pass')].groupby(['game_id']).agg({'complete_pass': 'sum', 'play_type': 'count'}).reset_index()
    away_pass_cmp_perc['off_pass_cmp_perc_away'] = (away_pass_cmp_perc['complete_pass'] / away_pass_cmp_perc['play_type']) * 100
    
    home_pass_cmp_perc = home_pass_cmp_perc[['game_id', 'off_pass_cmp_perc_home']]
    away_pass_cmp_perc = away_pass_cmp_perc[['game_id', 'off_pass_cmp_perc_away']]
    
    # Merge all the dataframes with the game_ids DataFrame
    offensive_metrics_df = game_ids.merge(home_fg, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_fg, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_fg_missed, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_fg_missed, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_kickoff_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_kickoff_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_kickret_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_kickret_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_puntret_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_puntret_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_pass_attempts, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_pass_attempts, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_passing_cmp, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_passing_cmp, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_passing_int, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_passing_int, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_passing_sacks, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_passing_sacks, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_passing_tds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_passing_tds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_passing_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_passing_yds, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(home_pass_cmp_perc, on='game_id', how='left')
    offensive_metrics_df = offensive_metrics_df.merge(away_pass_cmp_perc, on='game_id', how='left')
    
    off_agg = data.groupby(['game_id', 'posteam', 'posteam_type']).agg({
        'first_down_rush': 'sum',
        'first_down_pass': 'sum',
        'first_down_penalty': 'sum',
        'third_down_converted': 'sum',
        'fourth_down_converted': 'sum',
        'incomplete_pass': 'sum',
        'rush_attempt': 'sum',
        'pass_attempt': 'sum',
        'sack': 'sum',
        'touchdown': 'sum',
        'pass_touchdown': 'sum',
        'rush_touchdown': 'sum',
        'interception': 'sum',
        'fumble_lost': 'sum',
        'punt_inside_twenty': 'sum',
        'tackled_for_loss': 'sum'
    }).reset_index()

    off_agg['TOTAL_off_aggregated'] = off_agg.iloc[:, 3:].sum(axis=1)
    
    home_off_agg = off_agg[off_agg['posteam_type'] == 'home'].drop(columns=['posteam_type'])
    away_off_agg = off_agg[off_agg['posteam_type'] == 'away'].drop(columns=['posteam_type'])
    
    home_off_agg.columns = ['game_id', 'home_team'] + [f'{col}_home' for col in home_off_agg.columns if col not in ['game_id', 'posteam']]
    away_off_agg.columns = ['game_id', 'away_team'] + [f'{col}_away' for col in away_off_agg.columns if col not in ['game_id', 'posteam']]

    # Merge new metrics with existing offensive_metrics_df
    offensive_metrics_df = pd.merge(offensive_metrics_df, home_off_agg, on=['game_id', 'home_team'], how='left')
    offensive_metrics_df = pd.merge(offensive_metrics_df, away_off_agg, on=['game_id', 'away_team'], how='left')
    # Fill missing values with 0
    offensive_metrics_df = offensive_metrics_df.fillna(0)
    
    return offensive_metrics_df

def calculate_defensive_metrics(data: pd.DataFrame) -> pd.DataFrame:
    '''
    Calculate defensive metrics for each team.
    
    Args:
        data (pd.DataFrame): DataFrame containing play-by-play data.
        
    Returns:
        pd.DataFrame: DataFrame with defensive metrics for each team.
    '''
    
    # Create a DataFrame with unique game_id values
    #game_ids = pd.DataFrame({'game_id': data['game_id'].unique()})
    game_ids = data[['game_id', 'home_team', 'away_team']].drop_duplicates()
    
    # Tackles
    home_tackles = data[(data['posteam_type'] == 'away') & (data['solo_tackle'] == 1)].groupby(['game_id']).size().reset_index()
    away_tackles = data[(data['posteam_type'] == 'home') & (data['solo_tackle'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_tackles.columns = ['game_id', 'def_tackles_home']
    away_tackles.columns = ['game_id', 'def_tackles_away']
    
    # Sacks
    home_sacks = data[(data['posteam_type'] == 'away') & (data['sack'] == 1)].groupby(['game_id']).size().reset_index()
    away_sacks = data[(data['posteam_type'] == 'home') & (data['sack'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_sacks.columns = ['game_id', 'def_sacks_home']
    away_sacks.columns = ['game_id', 'def_sacks_away']
    
    # Interceptions
    home_interceptions = data[(data['posteam_type'] == 'away') & (data['interception'] == 1)].groupby(['game_id']).size().reset_index()
    away_interceptions = data[(data['posteam_type'] == 'home') & (data['interception'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_interceptions.columns = ['game_id', 'def_interceptions_home']
    away_interceptions.columns = ['game_id', 'def_interceptions_away']
    
    # Forced fumbles
    home_forced_fumbles = data[(data['posteam_type'] == 'away') & (data['fumble_forced'] == 1)].groupby(['game_id']).size().reset_index()
    away_forced_fumbles = data[(data['posteam_type'] == 'home') & (data['fumble_forced'] == 1)].groupby(['game_id']).size().reset_index()
    
    home_forced_fumbles.columns = ['game_id', 'def_forced_fumbles_home']
    away_forced_fumbles.columns = ['game_id', 'def_forced_fumbles_away']
    
    # Fumble recoveries
    home_fumble_recoveries = data[(data['posteam_type'] == 'away') & (data['fumble_recovery_1_team'] == 'home')].groupby(['game_id']).size().reset_index()
    away_fumble_recoveries = data[(data['posteam_type'] == 'home') & (data['fumble_recovery_1_team'] == 'away')].groupby(['game_id']).size().reset_index()
    
    home_fumble_recoveries.columns = ['game_id', 'def_fumble_recoveries_home']
    away_fumble_recoveries.columns = ['game_id', 'def_fumble_recoveries_away']
    
    
    # Merge all the dataframes with the game_ids DataFrame
    defensive_metrics_df = game_ids.merge(home_tackles, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(away_tackles, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(home_sacks, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(away_sacks, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(home_interceptions, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(away_interceptions, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(home_forced_fumbles, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(away_forced_fumbles, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(home_fumble_recoveries, on='game_id', how='left')
    defensive_metrics_df = defensive_metrics_df.merge(away_fumble_recoveries, on='game_id', how='left')
    
    def_qbhit_home = data[(data['posteam_type'] == 'away') & (data['qb_hit'] == 1)].groupby(['game_id']).size().reset_index(name='def_defense_qbhit_home')
    def_qbhit_away = data[(data['posteam_type'] == 'home') & (data['qb_hit'] == 1)].groupby(['game_id']).size().reset_index(name='def_defense_qbhit_away')

    def_safety_home = data[(data['posteam_type'] == 'away') & (data['safety'] == 1)].groupby(['game_id']).size().reset_index(name='def_defense_safety_home')
    def_safety_away = data[(data['posteam_type'] == 'home') & (data['safety'] == 1)].groupby(['game_id']).size().reset_index(name='def_defense_safety_away')

    def_agg = data.groupby(['game_id', 'posteam_type']).agg({
        'qb_hit': 'sum',
        'sack': 'sum',
        'fumble_forced': 'sum',
        'interception': 'sum',
        'safety': 'sum'
    }).reset_index()

    def_agg['TOTAL_def_aggregated'] = def_agg.iloc[:, 2:].sum(axis=1)
    
    home_def_agg = def_agg[def_agg['posteam_type'] == 'away'].drop(columns=['posteam_type'])
    away_def_agg = def_agg[def_agg['posteam_type'] == 'home'].drop(columns=['posteam_type'])
    
    home_def_agg.columns = ['game_id'] + [f'{col}_home' for col in home_def_agg.columns if col != 'game_id']
    away_def_agg.columns = ['game_id'] + [f'{col}_away' for col in away_def_agg.columns if col != 'game_id']

    # Merge new metrics with existing defensive_metrics_df
    defensive_metrics_df = pd.merge(defensive_metrics_df, def_qbhit_home, on='game_id', how='left')
    defensive_metrics_df = pd.merge(defensive_metrics_df, def_qbhit_away, on='game_id', how='left')
    defensive_metrics_df = pd.merge(defensive_metrics_df, def_safety_home, on='game_id', how='left')
    defensive_metrics_df = pd.merge(defensive_metrics_df, def_safety_away, on='game_id', how='left')
    defensive_metrics_df = pd.merge(defensive_metrics_df, home_def_agg, on='game_id', how='left')
    defensive_metrics_df = pd.merge(defensive_metrics_df, away_def_agg, on='game_id', how='left')
    
    # Fill missing values with 0
    defensive_metrics_df = defensive_metrics_df.fillna(0)
    
    return defensive_metrics_df
