docker login --username $GITHUB_ACTOR --password $GITHUB_TOKEN ghcr.io
docker pull ghcr.io/kestra-io/kestra-ee:v1.0.0
