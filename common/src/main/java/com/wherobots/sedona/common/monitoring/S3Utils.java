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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This implements S3 client using AWS Java SDK v2 which features async operations
 * This is to upload log to Wherobots S3 bucket asycly
 */
public class S3Utils
{

    public static S3AsyncClient getAsyncClient(String accessKey, String secretKey, String region) {
        return S3AsyncClient.builder().credentialsProvider(getProvider(accessKey, secretKey)).region(Region.of(region)).build();
    }

    public static S3Client getSyncClient(String accessKey, String secretKey, String region) {
        return S3Client.builder().credentialsProvider(getProvider(accessKey, secretKey)).region(Region.of(region)).build();
    }

    private static boolean doesBucketExist(S3Client s3, String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            s3.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    public static Bucket getBucket(S3Client s3, String bucket_name) {
        Bucket named_bucket = null;
        ListBucketsResponse buckets = s3.listBuckets();
        for (Bucket b : buckets.buckets()) {
            if (b.name().equals(bucket_name)) {
                named_bucket = b;
            }
        }
        return named_bucket;
    }

    public static Bucket createBucket(S3Client s3, String bucket_name, Logger logger) {
        Bucket b = null;
        // S3 bucket name should be unique in global space
        if (doesBucketExist(s3, bucket_name)) {
            logger.info(String.format("Bucket %s already exists.", bucket_name));
            b = getBucket(s3, bucket_name);
        } else {
            CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucket_name).build();
            CreateBucketResponse response = s3.createBucket(request);
        }
        logger.info("Created Bucket: " + bucket_name);
        return b;
    }

    public static PutObjectResponse putObject(S3Client s3, String bucketName, String objectKey, String content) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName).key(objectKey).build();
        return s3.putObject(putObjectRequest, RequestBody.fromString(content));
    }

    public static boolean putObject(S3AsyncClient s3, String bucketName, String objectKey, String content) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName).key(objectKey).build();
        CompletableFuture<PutObjectResponse> result = s3.putObject(putObjectRequest, AsyncRequestBody.fromString(content));
//        System.out.println("Uploading object " + objectKey + " to bucket " + bucketName);
        result.whenComplete((resp, err) -> {
            if (resp != null) {
//                System.out.println("Object uploaded. Details: " + resp);
            } else {
                // Handle error
                err.printStackTrace();
            }
        });
        result.join();
        return true;
    }

    public static String getObject(S3Client s3, String bucketName, String objectKey, Logger logger) {
        ResponseInputStream<GetObjectResponse> object = s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build());
        GetObjectResponse response = object.response();
        String content = new BufferedReader(
                new InputStreamReader(object, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
        return content;
    }

    public static ListObjectsV2Response listObject(S3Client s3, String bucketName, String prefix, Logger logger) {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
        ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
        return listObjectsV2Response;
    }

    public static void deleteBucket(S3Client s3, String bucket_name, Logger logger) {
        logger.info("Deleting S3 bucket: " + bucket_name);
        logger.info(" - removing objects from bucket");
        ListObjectsResponse object_listing = s3.listObjects(ListObjectsRequest.builder().bucket(bucket_name).build());
        while (true) {
            for (S3Object content : object_listing.contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket_name).key(content.key()).build());
            }

            // more object_listing to retrieve?
            if (object_listing.isTruncated()) {
                object_listing = s3.listObjects(ListObjectsRequest.builder().bucket(bucket_name).marker(object_listing.nextMarker()).build());
            } else {
                break;
            }
        }
        logger.info(" OK, bucket ready to delete!");
        s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket_name).build());
        logger.info("Done!");
    }

    public static AwsCredentialsProvider getProvider(String accessKey, String secretKey) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                accessKey,
                secretKey);
        return StaticCredentialsProvider.create(awsCreds);
    }
}