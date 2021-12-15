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
package org.ehcache.xml;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

public class XmlModel {
  public static TemporalUnit convertToJavaTimeUnit(org.ehcache.xml.model.TimeUnit unit) {
    switch (unit) {
      case NANOS:
        return ChronoUnit.NANOS;
      case MICROS:
        return ChronoUnit.MICROS;
      case MILLIS:
        return ChronoUnit.MILLIS;
      case SECONDS:
        return ChronoUnit.SECONDS;
      case MINUTES:
        return ChronoUnit.MINUTES;
      case HOURS:
        return ChronoUnit.HOURS;
      case DAYS:
        return ChronoUnit.DAYS;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public static TimeUnit convertToJUCTimeUnit(org.ehcache.xml.model.TimeUnit unit) {
    switch (unit) {
      case NANOS:
        return TimeUnit.NANOSECONDS;
      case MICROS:
      return TimeUnit.MICROSECONDS;
      case MILLIS:
        return TimeUnit.MILLISECONDS;
      case SECONDS:
        return TimeUnit.SECONDS;
      case MINUTES:
        return TimeUnit.MINUTES;
      case HOURS:
        return TimeUnit.HOURS;
      case DAYS:
        return TimeUnit.DAYS;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public static TemporalUnit convertToJavaTemporalUnit(org.ehcache.xml.model.TimeUnit unit) {
    switch (unit) {
      case NANOS:
        return ChronoUnit.NANOS;
      case MICROS:
      return ChronoUnit.MICROS;
      case MILLIS:
        return ChronoUnit.MILLIS;
      case SECONDS:
        return ChronoUnit.SECONDS;
      case MINUTES:
        return ChronoUnit.MINUTES;
      case HOURS:
        return ChronoUnit.HOURS;
      case DAYS:
        return ChronoUnit.DAYS;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  public static org.ehcache.xml.model.TimeUnit convertToXmlTimeUnit(TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return org.ehcache.xml.model.TimeUnit.NANOS;
      case MICROSECONDS:
        return org.ehcache.xml.model.TimeUnit.MICROS;
      case MILLISECONDS:
        return org.ehcache.xml.model.TimeUnit.MILLIS;
      case SECONDS:
        return org.ehcache.xml.model.TimeUnit.SECONDS;
      case MINUTES:
        return org.ehcache.xml.model.TimeUnit.MINUTES;
      case HOURS:
        return org.ehcache.xml.model.TimeUnit.HOURS;
      case DAYS:
        return org.ehcache.xml.model.TimeUnit.DAYS;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

}
