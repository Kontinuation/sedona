# This package is for integration test on Snowflake

To execute the test locally, copy the following template and replace the values with your own credentials.
``` bash
export SNOWFLAKE_AUTH_METHOD=BASIC
export SNOWFLAKE_USER=""
export SNOWFLAKE_PASSWORD=""
export SNOWFLAKE_DB=WHEROBOTS_PLAYGROUND
export SNOWFLAKE_SCHEMA=SEDONA
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_ROLE=""
export SNOWFLAKE_ACCOUNT=""
export SNOWFLAKE_GEOTOOLS_VERSION=1.4.0-28.2
mvn clean package -DskipTests -pl snowflake -am
mkdir snowflake-tester/tmp
cp snowflake/target/sedona-snowflake-*.jar snowflake-tester/tmp/
wget -O https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${SNOWFLAKE_GEOTOOLS_VERSION}/geotools-wrapper-${SNOWFLAKE_GEOTOOLS_VERSION}.jar -P snowflake-tester/tmp/
export SEDONA_VERSION=$(grep version pom.xml | grep -v -e '<?xml|~'| head -n 1 | sed 's/[[:space:]]//g' | sed -E 's/<.{0,1}version>//g') && echo $SEDONA_VERSION && mvn test -P snowflake -am
```
