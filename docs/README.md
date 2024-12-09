# Osires

Open Services for Interoperable and Reproducible research based on Spatiotemporal data

## Getting started

To Locally develop & test run:
> docker compose up

To remotley push you modifications push on the main branch your markdown code and you can check at 
https://geo-ord.gitlab.io/osires/

## To remove all images and volumes
docker compose -f dev_docker-compose.yml down -v --rmi local
