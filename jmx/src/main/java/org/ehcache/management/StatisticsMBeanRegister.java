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

package org.ehcache.management;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.ehcache.Cache;

/**
 * Register tool to allow users register their cache for statistics mbean
 * monitoring
 * 
 * @author Hung Huynh
 *
 */
public class StatisticsMBeanRegister {
  public static final String DEFAULT_MBEAN_DOMAIN = "org.ehcache";
  public static final String JSR107_MBEAN_DOMAIN = "javax.cache";

  private StatisticsMBeanRegister() {
    // not to be instantiated
  }

  private static ObjectName createObjectName(String domain, String cacheName) {
    try {
      return new ObjectName(domain + ":type=CacheStatistics,Cache=" + cacheName);
    } catch (MalformedObjectNameException e) {
      throw new AssertionError();
    }
  }

  /**
   * Register your cache for statistics monitoring via JMX bean
   * 
   * The mbean domain will be the default org.ehcache
   * 
   * @param cache
   *          the cache
   * @param cacheName
   *          name of the cache
   */
  public static void registerStatisticsMbean(Cache<?, ?> cache, String cacheName)
      throws InstanceAlreadyExistsException, MBeanRegistrationException {
    registerStatisticsMbean(cache, cacheName, DEFAULT_MBEAN_DOMAIN);
  }

  /**
   * Register your cache for statistics monitoring via JMX bean, specifying cache name and domain name
   * 
   * @param cache
   *          the cache
   * @param cacheName
   *          name of the cache
   * @param domain
   *          domain name of the mbean
   */
  public static void registerStatisticsMbean(Cache<?, ?> cache, String cacheName, String domain)
      throws InstanceAlreadyExistsException, MBeanRegistrationException {
    try {
      ObjectName mBeanName = createObjectName(domain, cacheName);
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      CacheStatisticsMXBean statisticsMXBean = new CacheStatisticsMXBeanImpl(cache);
      mbs.registerMBean(statisticsMXBean, mBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new AssertionError();
    }
  }
}
