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

import java.util.concurrent.TimeUnit;

public class XmlModel {
  public static TimeUnit convertToJavaTimeUnit(org.ehcache.xml.model.TimeUnit unit) {
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
}
