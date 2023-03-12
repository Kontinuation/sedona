/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wherobots.sedona.common.monitoring;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CloudWatchUtils
{
    public static CloudWatchAsyncClient getClient(String accessKey, String secretKey, String region){
        CloudWatchAsyncClient client = CloudWatchAsyncClient.builder()
                .credentialsProvider(getProvider(accessKey, secretKey)).region(Region.of(region)).build();
        return client;
    }
    public static void putMetric(CloudWatchAsyncClient client, String namespace, Map<String, Double> dataPoints,
            String metricName, String dimensionName) {
        // Set an Instant object.
        Instant instant = Instant.now();
        List<MetricDatum> datums = new ArrayList<>();
        try {
            for (Map.Entry<String, Double> entry : dataPoints.entrySet()) {
                Dimension dimension = Dimension.builder()
                        .name(dimensionName)
                        .value(entry.getKey())
                        .build();
                datums.add(MetricDatum.builder()
                        .metricName(metricName)
                        .unit(StandardUnit.NONE)
                        .value(entry.getValue())
                        .timestamp(instant)
                        .dimensions(dimension).build());
            }

            PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace(namespace)
                    .metricData(datums).build();

            CompletableFuture<PutMetricDataResponse> result = client.putMetricData(request);
//            result.whenComplete((resp, err) -> {
//                try {
//                    if (resp != null) {
//                        System.out.println("Object uploaded. Details: " + resp);
//                    } else {
//                        // Handle error
//                        err.printStackTrace();
//                    }
//                } finally {
//                    // Only close the client when you are completely done with it
//                    //                client.close();
//                }
//            });
//            result.join();
        } catch (CloudWatchException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static AwsCredentialsProvider getProvider(String accessKey, String secretKey) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                accessKey,
                secretKey);
        return StaticCredentialsProvider.create(awsCreds);
    }
}
