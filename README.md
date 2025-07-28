<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Sedona enterprise

## Compile

See: https://sedona.apache.org/latest/setup/compile/

## Develop

See: https://sedona.apache.org/latest/community/develop/

## Release

Scala 2.12, Spark 3.3

```
mvn clean deploy -DskipTests
```

Scala 2.13, Spark 3.3

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

## Local development environment

To build the Wherobots project locally, please add the following in your local `~/.m2/settings.xml` . You should give your `YOUR_GITHUB_PERSIONAL_ACCESS_TOKEN` permission to read the organization package.

```
<settings>
  <servers>
    <server>
      <id>github</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>YOUR_GITHUB_PERSIONAL_ACCESS_TOKEN</password>
    </server>
  </servers>
</settings>
```

To release and publish this project, `YOUR_GITHUB_PERSIONAL_ACCESS_TOKEN` in `~/.m2/settings.xml` should have permission to write to the organization package.
