from src.data_loader import load_and_clean_data, save_processed_data
from src.feature_calculator import calculate_post_priori, save_post_priori
from src.scenario_analyser import calculate_scenario_averages, calculate_mutual_game_averages, save_feature_engineered

def main():
    raw_file_path = r'data/raw/NFL Play by Play 2009-2018 (v5).csv'
    processed_file_path_post_priori = r'data/processed/post_priori.csv'
    processed_file_path_features = r'data/processed/feature_engineered.csv'

    nfl_data_cleaned = load_and_clean_data(raw_file_path)
    post_priori_df = calculate_post_priori(nfl_data_cleaned)

    save_post_priori(post_priori_df, processed_file_path_post_priori)

    # current_season_avgs = calculate_scenario_averages(post_priori_df)
    # mutual_game_avgs = calculate_mutual_game_averages(post_priori_df)

    # save_feature_engineered(current_season_avgs, processed_file_path_features)

if __name__ == "__main__":
    main()