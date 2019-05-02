/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.core.statistics;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration of all latency histograms.
 */
public class LatencyHistogramConfiguration {

  public static final double DEFAULT_PHI = 0.63;
  public static final int DEFAULT_BUCKET_COUNT = 20;
  public static final Duration DEFAULT_WINDOW = Duration.ofMinutes(1) ;

  public static final LatencyHistogramConfiguration DEFAULT = new LatencyHistogramConfiguration(DEFAULT_PHI, DEFAULT_BUCKET_COUNT, DEFAULT_WINDOW);

  private final double phi;
  private final int bucketCount;
  private final Duration window;

  /**
   * Default constructor.
   *
   * @param phi histogram bucket bias factor
   * @param bucketCount number of buckets
   * @param window sliding window size
   */
  public LatencyHistogramConfiguration(double phi, int bucketCount, Duration window) {
    this.phi = phi;
    this.bucketCount = bucketCount;
    this.window = Objects.requireNonNull(window);
  }

  public double getPhi() {
    return phi;
  }

  public int getBucketCount() {
    return bucketCount;
  }

  public Duration getWindow() {
    return window;
  }

  @Override
  public String toString() {
    return "LatencyHistogramConfiguration{" +
           "phi=" + phi +
           ", bucketCount=" + bucketCount +
           ", window=" + window +
           '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LatencyHistogramConfiguration that = (LatencyHistogramConfiguration) o;

    if (that.phi != phi) return false;
    if (bucketCount != that.bucketCount) return false;
    return window.equals(that.window);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(phi);
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + bucketCount;
    result = 31 * result + window.hashCode();
    return result;
  }
}
