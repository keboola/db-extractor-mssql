docker pull quay.io/keboola/developer-portal-cli-v2:latest
export REPOSITORY=`docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL \
  quay.io/keboola/developer-portal-cli-v2:latest ecr:get-repository keboola keboola.ex-db-mssql`
docker tag keboola/ex-db-mssql:latest $REPOSITORY:$TRAVIS_TAG
docker tag keboola/ex-db-mssql:latest $REPOSITORY:latest
eval $(docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL \
  quay.io/keboola/developer-portal-cli-v2:latest ecr:get-login keboola keboola.ex-db-mssql)
docker push $REPOSITORY:$TRAVIS_TAG
docker push $REPOSITORY:latest

export SYRUP_CLI=quay.io/keboola/syrup-cli

docker pull $SYRUP_CLI:latest
# run simple job
docker run --rm -e KBC_STORAGE_TOKEN=$KBC_SYRUP_CLI_TOKEN \
   $SYRUP_CLI:latest run-job keboola.ex-db-mssql 287615945 $TRAVIS_TAG

if [ $? -ne 0 ]; then
  echo 'Simple test job run failed'
  exit 1;
fi

#run ssh job
docker run --rm -e KBC_STORAGE_TOKEN=$KBC_SYRUP_CLI_TOKEN \
   $SYRUP_CLI:latest run-job keboola.ex-db-mssql 287628364 $TRAVIS_TAG

if [ $? -ne 0 ]; then
  echo 'SSH test job run failed'
  exit 1;
fi

# If those jobs pass then go ahead and update the DP tag
docker run --rm \
  -e KBC_DEVELOPERPORTAL_USERNAME=$KBC_DEVELOPERPORTAL_USERNAME \
  -e KBC_DEVELOPERPORTAL_PASSWORD=$KBC_DEVELOPERPORTAL_PASSWORD \
  -e KBC_DEVELOPERPORTAL_URL=$KBC_DEVELOPERPORTAL_URL \
  quay.io/keboola/developer-portal-cli-v2:latest update-app-repository keboola keboola.ex-db-mssql $TRAVIS_TAG

