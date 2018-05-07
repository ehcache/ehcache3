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

package org.ehcache.xml.model;

import org.ehcache.xml.XmlModel;

import java.time.temporal.TemporalUnit;

public class Expiry {

  private final ExpiryType type;

  public Expiry(final ExpiryType type) {
    this.type = type;
  }

  public boolean isUserDef() {
    return type != null && type.getClazz() != null;
  }

  public boolean isTTI() {
    return type != null && type.getTti() != null;
  }

  public boolean isTTL() {
    return type != null && type.getTtl() != null;
  }

  public String type() {
    return type.getClazz();
  }

  public long value() {
    final TimeType time;
    if(isTTI()) {
      time = type.getTti();
    } else {
      time = type.getTtl();
    }
    return time == null ? 0L : time.getValue().longValue();
  }

  public TemporalUnit unit() {
    final TimeType time;
    if(isTTI()) {
      time = type.getTti();
    } else {
      time = type.getTtl();
    }
    if(time != null) {
      return XmlModel.convertToJavaTemporalUnit(time.getUnit());
    }
    return null;
  }
}
