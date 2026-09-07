docker login --username $GITHUB_ACTOR --password $GITHUB_TOKEN ghcr.io

KESTRA_IMAGE_VERSION=$(grep -oP '^kestraImageVersion=\K.*' gradle.properties)
docker pull ghcr.io/kestra-io/kestra-ee:v${KESTRA_IMAGE_VERSION}-no-plugins