# Check if device is a macbook with m1 chip

if [[ $(uname -m) == "arm64" ]]; then
    export DOCKER_DEFAULT_PLATFORM=linux/amd64
fi

docker compose -f dev_docker-compose.yml up --build