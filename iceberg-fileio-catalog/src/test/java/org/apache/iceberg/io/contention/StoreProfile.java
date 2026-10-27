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
package org.apache.iceberg.io.contention;

import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * An object store's request latencies, as endive models them (endive/providers/*.toml, the
 * simulator's calibration, cited there): a GET and a PUT are lognormal about a base plus a cost per
 * MiB, a CAS is lognormal about its median and no faster than a PUT of its bytes, and a conditional
 * append is lognormal about its median when it lands and about another when it loses its race.
 * Every latency is at least the store's floor. Whether the store appends is the provider's: S3
 * Standard and GCS do not, and the catalog compacts on every commit there.
 */
final class StoreProfile {

  /** A lognormal latency about {@code base + perMiB * MiB}, in ms. */
  static final class Dist {
    final double baseMs;
    final double perMiBMs;
    final double sigma;

    Dist(double baseMs, double perMiBMs, double sigma) {
      this.baseMs = baseMs;
      this.perMiBMs = perMiBMs;
      this.sigma = sigma;
    }

    double sampleMs(long bytes, double floorMs, SplittableRandom random) {
      double center = baseMs + perMiBMs * bytes / (1024.0 * 1024.0);
      if (center <= 0) {
        return floorMs;
      }
      double sample = sigma > 0 ? center * Math.exp(sigma * gaussian(random)) : center;
      return Math.max(sample, floorMs);
    }
  }

  final String name;
  final double floorMs;
  final Dist read;
  final Dist write;
  final Dist cas;
  final Dist append;
  final Dist appendFailure;
  final boolean supportsAppend;

  private StoreProfile(
      String name, double floorMs, Dist read, Dist write, Dist cas, Dist append,
      Dist appendFailure) {
    this.name = name;
    this.floorMs = floorMs;
    this.read = read;
    this.write = write;
    this.cas = cas;
    this.append = append;
    this.appendFailure = appendFailure;
    this.supportsAppend = append != null;
  }

  /** endive's profiles (endive/endive/providers/*.toml, 2026-04). */
  private static final Map<String, StoreProfile> PROFILES =
      Map.of(
          "s3",
          new StoreProfile("s3", 10, new Dist(27, 0, 0.62), new Dist(60, 20, 0.29),
              new Dist(61, 0, 0.14), null, null),
          "s3x",
          new StoreProfile("s3x", 1, new Dist(2.5, 0, 0.57), new Dist(6.5, 10, 0.24),
              new Dist(22, 0, 0.22), new Dist(21, 0, 0.25), new Dist(23, 0, 0.22)),
          "azure",
          new StoreProfile("azure", 20, new Dist(38, 0, 0.66), new Dist(45, 25, 0.50),
              new Dist(93, 0, 0.82), new Dist(87, 0, 0.28), new Dist(2072, 0, 0.68)),
          "azurex",
          new StoreProfile("azurex", 20, new Dist(35, 0, 0.08), new Dist(41, 15, 0.10),
              new Dist(64, 0, 0.73), new Dist(70, 0, 0.23), new Dist(2534, 0, 0.65)),
          "gcp",
          new StoreProfile("gcp", 80, new Dist(200, 0, 0.30), new Dist(200, 17, 0.30),
              new Dist(170, 0, 0.91), null, null),
          "instant",
          new StoreProfile("instant", 1, new Dist(1, 0, 0.1), new Dist(0.5, 0.1, 0.1),
              new Dist(1, 0, 0.1), new Dist(1, 0, 0.1), new Dist(1, 0, 0.1)),
          "none",
          new StoreProfile("none", 0, new Dist(0, 0, 0), new Dist(0, 0, 0), new Dist(0, 0, 0),
              new Dist(0, 0, 0), new Dist(0, 0, 0)));

  /** The profile named {@code name}: s3, s3x, azure, azurex, gcp, instant, or none (no latency). */
  static StoreProfile named(String name) {
    StoreProfile profile = PROFILES.get(name.toLowerCase(Locale.ROOT));
    if (profile == null) {
      throw new IllegalArgumentException(
          "Unknown store profile: " + name + " (known: " + PROFILES.keySet() + ")");
    }
    return profile;
  }

  double readMs(long bytes, SplittableRandom random) {
    return read.sampleMs(bytes, floorMs, random);
  }

  double writeMs(long bytes, SplittableRandom random) {
    return write.sampleMs(bytes, floorMs, random);
  }

  /** A CAS of {@code bytes}: its own latency, or a PUT of its bytes if that is slower. */
  double casMs(long bytes, SplittableRandom random) {
    return Math.max(cas.sampleMs(0, floorMs, random), writeMs(bytes, random));
  }

  double appendMs(SplittableRandom random) {
    return append.sampleMs(0, floorMs, random);
  }

  double appendFailureMs(SplittableRandom random) {
    return appendFailure.sampleMs(0, floorMs, random);
  }

  /** A standard normal draw (Box-Muller). */
  private static double gaussian(SplittableRandom random) {
    double u = 1.0 - random.nextDouble();
    double v = random.nextDouble();
    return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
  }
}
