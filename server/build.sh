sudo docker stop travelnet travelnet-dashboard
docker rm travelnet travelnet-dashboard
docker compose build
docker compose up -d