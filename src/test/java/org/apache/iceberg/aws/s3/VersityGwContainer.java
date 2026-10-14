/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.s3;

import java.net.URI;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * S3 emulator for the integration suites: versitygw serving a POSIX backend on a tmpfs.
 * Replaces MinIO, whose images are no longer published (removed from Docker Hub in
 * 2026-09, login-only on quay.io).
 *
 * <p>versitygw evaluates {@code If-Match} / {@code If-None-Match: *} on PutObject and the
 * write under one file lock, so concurrent conditional writes have exactly one winner.
 * It has no S3 Express semantics: {@code writeOffsetBytes} is accepted and ignored. See
 * {@link VersityGwEmulatorCanaries} for both.
 */
public class VersityGwContainer extends GenericContainer<VersityGwContainer> {

  // Pinned so canary results are reproducible; bumping the tag is the trigger to re-check
  // VersityGwEmulatorCanaries.
  public static final String IMAGE = "versity/versitygw:v1.9.0";
  public static final String ACCESS_KEY = "fileio-catalog";
  public static final String SECRET_KEY = "fileio-catalog-secret";
  private static final int PORT = 7070;

  public VersityGwContainer() {
    super(DockerImageName.parse(IMAGE));
    withExposedPorts(PORT);
    withEnv("ROOT_ACCESS_KEY", ACCESS_KEY);
    withEnv("ROOT_SECRET_KEY", SECRET_KEY);
    withTmpFs(Map.of("/data", "rw"));
    withCommand("posix", "/data");
    // Any HTTP answer (an unauthenticated request gets 403) means the gateway is serving.
    waitingFor(Wait.forHttp("/").forPort(PORT).forStatusCodeMatching(code -> code > 0));
  }

  public URI endpoint() {
    return URI.create("http://" + getHost() + ":" + getMappedPort(PORT));
  }

  /** A new client; the caller owns it (S3FileIO closes the client it is given). */
  public static S3Client createS3Client(URI endpoint) {
    return S3Client.builder()
        .endpointOverride(endpoint)
        .region(Region.US_EAST_1)
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .build();
  }
}
