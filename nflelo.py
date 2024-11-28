import pandas as pd
import random
from neo4j import GraphDatabase

class Neo4jElo:
    """
    A class to interact with Neo4j database for ELO rating calculation and prediction.
    """

    def __init__(self, uri, user, password):
        """
        Initialize the connection to the Neo4j database.
        
        Args:
            uri (str): URI of the Neo4j instance.
            user (str): Username for Neo4j authentication.
            password (str): Password for Neo4j authentication.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the connection to the Neo4j database."""
        self.driver.close()

    def create_teams(self, teams):
        """
        Create nodes for all teams in the NFL dataset.
        
        Args:
            teams (list): List of unique team names.
        """
        with self.driver.session() as session:
            for team in teams:
                session.run("MERGE (t:Team {name: $name})", name=team)

    def create_game(self, game_id, home_team, away_team, home_score, away_score):
        """
        Create relationships between teams based on game results.
        
        Args:
            game_id (int): Unique identifier for the game.
            home_team (str): Name of the home team.
            away_team (str): Name of the away team.
            home_score (int): Score of the home team.
            away_score (int): Score of the away team.
        """
        with self.driver.session() as session:
            session.run("""
                MATCH (home:Team {name: $home_team}), (away:Team {name: $away_team})
                MERGE (home)-[:PLAYED {game_id: $game_id, score: $home_score}]->(away)
                MERGE (away)-[:PLAYED {game_id: $game_id, score: $away_score}]->(home)
            """, game_id=game_id, home_team=home_team, away_team=away_team,
            home_score=home_score, away_score=away_score)

    def initialize_elo(self):
        """Initialize ELO ratings for all teams in the database."""
        with self.driver.session() as session:
            session.run("MATCH (t:Team) SET t.elo = 1500")

    def calculate_elo(self, k=20):
        """
        Calculate and update ELO ratings after each game.
        
        Args:
            k (int): K-factor for ELO rating calculation.
        """
        with self.driver.session() as session:
            # Initialize ELO ratings if not already done
            self.initialize_elo()
            
            # Retrieve all games from the database
            games = session.run("""
            MATCH (home:Team)-[r1:PLAYED]->(away:Team)-[r2:PLAYED]->(home)
            RETURN home.name AS home_team, away.name AS away_team,
                r1.score AS home_score, r2.score AS away_score
        """)
            
            for record in games:
                home_team = record["home_team"]
                away_team = record["away_team"]
                home_score = record["home_score"]
                
                # Fetch current ELO ratings from Neo4j
                home_elo = session.run("MATCH (t:Team {name: $name}) RETURN t.elo AS elo", name=home_team).single()["elo"]
                away_elo = session.run("MATCH (t:Team {name: $name}) RETURN t.elo AS elo", name=away_team).single()["elo"]
                
                # Calculate expected scores
                expected_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
                expected_away = 1 / (1 + 10 ** ((home_elo - away_elo) / 400))
                
                # Determine actual scores
                actual_home = 1 if home_score > record["away_score"] else 0 if home_score < record["away_score"] else 0.5
                
                # Update ELO ratings
                new_home_elo = home_elo + k * (actual_home - expected_home)
                new_away_elo = away_elo + k * ((1 - actual_home) - expected_away)
                
                # Update the database with new ELO ratings
                session.run("MATCH (t:Team {name: $name}) SET t.elo = $elo", name=home_team, elo=new_home_elo)
                session.run("MATCH (t:Team {name: $name}) SET t.elo = $elo", name=away_team, elo=new_away_elo)

    def get_team_elos(self, home_team, away_team):
        """
        Query Neo4j to get current ELO ratings for both teams.
        
        Args:
            home_team (str): Name of the home team.
            away_team (str): Name of the away team.
        
        Returns:
            tuple: Current ELO ratings for both teams.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (home:Team {name: $home_team}), (away:Team {name: $away_team})
                RETURN home.elo AS home_elo, away.elo AS away_elo
            """, home_team=home_team, away_team=away_team)
            
            record = result.single()
            return record["home_elo"], record["away_elo"]

    def calculate_expected_scores(self, home_elo, away_elo):
        """
        Calculate expected scores based on ELO ratings.
        
        Args:
            home_elo (float): ELO rating of the home team.
            away_elo (float): ELO rating of the away team.
        
        Returns:
            tuple: Expected scores for both teams.
        """
        expected_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
        expected_away = 1 / (1 + 10 ** ((home_elo - away_elo) / 400))
        return expected_home, expected_away

    def predict_winner(self, home_team, away_team):
        """
        Predict winner based on current ELO ratings of both teams.
        
        Args:
            home_team (str): Name of the home team.
            away_team (str): Name of the away team.
        
        Returns:
            str: Predicted winner ('Home' or 'Away').
        """
        # Get current ELO ratings
        home_elo, away_elo = self.get_team_elos(home_team, away_team)
        
        # Calculate expected scores
        expected_home, _ = self.calculate_expected_scores(home_elo, away_elo)
        
        # Predict winner based on expected scores
        if expected_home > 0.5:
            return f"Predicted winner: {home_team} (Home)"
        else:
            return f"Predicted winner: {away_team} (Away)"

# Function to generate test data with random scores for four teams
def generate_test_data():
    teams = ['Team1', 'Team2', 'Team3', 'Team4', 'Team5']
    
    # Generate all possible combinations of matchups between these four teams
    games = []
    game_id_counter = 1
    
    for i in range(len(teams)):
        for j in range(len(teams)):
            if i != j:
                games.append({
                    'game_id': game_id_counter,
                    'home_team': teams[i],
                    'away_team': teams[j],
                    'home_score': random.randint(10, 40),  # Random score between 10 and 40
                    'away_score': random.randint(10, 40)   # Random score between 10 and 40
                })
                game_id_counter += 1
    
    return pd.DataFrame(games)

# Main function to execute the workflow with test data
def main():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"

    elo_system = Neo4jElo(uri, user, password)

    # # Generate test data with four teams playing all possible combinations of games
    # nfl_data = generate_test_data()

    # print("Test Data:\n", nfl_data)

    # # Create nodes for all teams
    # teams = pd.concat([nfl_data['home_team'], nfl_data['away_team']]).unique()
    # elo_system.create_teams(teams)

    # # Insert games into Neo4j and calculate ELOs
    # for _, row in nfl_data.iterrows():
    #     elo_system.create_game(row['game_id'], row['home_team'], row['away_team'], row['home_score'], row['away_score'])

    # elo_system.calculate_elo()

    # Predict a winner for an upcoming game between Team1 and Team2 as an example
    predicted_winner = elo_system.predict_winner('Team4', 'Team5')
    print(f"Predicted Winner between Team1 and Team2: {predicted_winner}")

    elo_system.close()

if __name__ == "__main__":
    main()