docker login --username $GITHUB_ACTOR --password $GITHUB_TOKEN ghcr.io

KESTRA_VERSION=$(grep -oP '^kestraVersion=\K.*' gradle.properties)
docker pull ghcr.io/kestra-io/kestra-ee:v${KESTRA_VERSION}-no-plugins