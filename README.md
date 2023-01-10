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


## Wherobots Enterprise release schedule

| version | Release date | Sedona enterprise version | Sedona open-source version | GeoLake version |  Lampy version  | GeoTorchAI version |
|:-------:|:------------:|:-------------------------:|:--------------------------:|:---------------:|:---------------:|:------------------:|
|  1.0.0  |      TBD     |      1.0.0 (03/2023)      |       1.4.0 (03/2023)      | 1.0.0 (03/2023) | 1.0.0 (03/2023) |   1.0.0 (03/2023)  |
|         |              |                           |                            |                 |                 |                    |


