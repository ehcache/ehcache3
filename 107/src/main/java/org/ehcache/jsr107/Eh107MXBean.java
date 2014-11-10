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
package org.ehcache.jsr107;

import javax.cache.CacheException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * @author teck
 */
abstract class Eh107MXBean {

  private final ObjectName objectName;

  Eh107MXBean(String cacheName, Eh107CacheManager cacheManager, final String beanName) {
    this.objectName = createObjectName(cacheName, cacheManager, beanName);
  }

  private String sanitize(String string) {
    return string == null ? "" : string.replaceAll(",|:|=|\n", ".");
  }

  private ObjectName createObjectName(String cacheName, Eh107CacheManager cacheManager, String beanName) {
    // XXX: Don't we need to unique-ify these by ClassLoader somehow too? 107
    // CacheManagers can be created
    // for the same URI but with different loaders

    String cacheManagerName = sanitize(cacheManager.getURI().toString());
    cacheName = sanitize(cacheName);

    try {
      return new ObjectName("javax.cache:type=Cache" + beanName + ",CacheManager=" + cacheManagerName + ",Cache="
          + cacheName);
    } catch (MalformedObjectNameException e) {
      throw new CacheException(e);
    }
  }

  ObjectName getObjectName() {
    return this.objectName;
  }

}
