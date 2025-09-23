## Setup

# Clone the repo locally
git clone https://github.com/vivadata/music-data-eng.git

# Go inside the repo
cd music_data_eng

# Create virtual environment
pyenv virtualenv 3.10.12 music-env

# Make the virtual env local
pyenv local music-env

# Environment variables
direnv allow  # approve .envrc content

## Git commands
git add .
git commit -m "Updated Makefile" # Example commit message
git push origin main

## Docker commands
docker compose up 
docker compose down
docker system prune
docker volume prune -f

## Airflow
airflow / airflow 
sudo chmod -R 777 dags/ # command to change permissions after changing the dags
chmod -R 777 logs/ 