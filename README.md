# Sedona enterprise

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
