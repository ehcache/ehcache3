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

package org.ehcache.core;

/**
 * @author rism
 */
public class CacheConfigurationChangeEvent {
  private final CacheConfigurationProperty property;
  private final Object newValue;
  private final Object oldValue;

  public CacheConfigurationChangeEvent(CacheConfigurationProperty property, Object oldValue, Object newValue) {

    this.property = property;
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  public CacheConfigurationProperty getProperty() {
    return this.property;
  }

  public Object getNewValue() {
    return this.newValue;
  }

  public Object getOldValue() {
    return this.oldValue;
  }
}
