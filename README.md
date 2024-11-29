# NFL-winnerPrediction_ELO-Neo4j
 
Predicting NFL Game Outcomes Using ELO Ratings and Machine Learning Models - Installation Guide

Prerequisites-
Python 3.8 or higher
Neo4j Database
Git (optional)

         -pandas
         -numpy
         -scikit-learn
         -torch
         -neo4j
         -category_encoders
         -matplotlib
         -seaborn

Neo4j Setup-
Download and install Neo4j Desktop from https://neo4j.com/download/
Create a new database with the following credentials:
URI: "bolt://localhost:7687"
Username: "neo4j"
Password: "password"

Troubleshooting-
Ensure Neo4j database is running before executing the ELO rating system
Check if all required Python packages are installed correctly
Verify the data directory structure and file locations

Additional Notes-
The project is configured to use the last 500 games for testing
Models are trained on data prior to 2018
Default hyperparameters can be modified in the respective Jupyter Notebook files.


