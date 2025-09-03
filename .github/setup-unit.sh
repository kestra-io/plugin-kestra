echo "---"
echo $GITHUB_ACTOR
echo "---"
echo $GITHUB_TOKEN
echo "---"

docker login --username $GITHUB_ACTOR --password $GITHUB_TOKEN ghcr.io
docker pull ghcr.io/kestra-io/kestra-ee:develop