# Sedona enterprise

## Compile

```
mvn clean install
```

## Release

Scala 2.12, Spark 3.0 - 3.3

```
mvn clean deploy -DskipTests
```

Scala 2.13, Spark 3.0 - 3.3

```
mvn clean deploy -DskipTests -Dscala=2.13
```

Scala 2.12, Spark 3.4+

```
mvn clean deploy -DskipTests -Dscala=2.12 -Dspark=3.4
```

Scala 2.13, Spark 3.4+

```
mvn clean deploy -DskipTests -Dscala=2.13 -Dspark=3.4
```
