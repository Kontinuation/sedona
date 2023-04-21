# Sedona enterprise

## Set up env

Sedona enterprise requires the following envs:

* `WHEROBOTS_USERID`: any value is allowed
* `WHEROBOTS_AWS_ACCESSKEY`: [See here](https://github.com/wherobots/sedona-enterprise/settings/variables/actions)
* `WHEROBOTS_AWS_SECRETKEY`: [See here](https://github.com/wherobots/sedona-enterprise/settings/variables/actions)
* `WHEROBOTS_AWS_S3BUCKET`: test-wherobots-user-logs/s3-log-tester
* `WHEROBOTS_AWS_REGION`: us-west-2
* `WHEROBOTS_PRODUCT`: Must follow this format: `wherobots=1.0.0+spark=3.3+geotools=1.4.0-28.2` or `sedona=1.0.0+spark=3.3+geotools=1.4.0-28.2`

The corresponding IAM username is `s3-log-tester`. It only has putObject and pubMetric permission to `test-wherobots-user-logs/s3-log-tester` S3 bucekt prefix and CloudWatch namespace, respectively. 

You can use `setup_wherobots_env.sh` in the main folder to set these env. But make sure you put the correct values inside.

## Compile

```
mvn clean install
```

## Release

Scala 2.12

```
mvn clean deploy -DskipTests
```

Scala 2.13

```
mvn clean deploy -DskipTests -Dscala=2.13
```


