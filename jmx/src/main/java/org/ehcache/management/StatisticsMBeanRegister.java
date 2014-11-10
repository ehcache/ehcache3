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
import java.net.URI;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
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
  public static final String JSR107_MBEAN_DOMAIN  = "javax.cache";

  private StatisticsMBeanRegister() {
    // not to be instantiated
  }

  /**
   * Create MBean object name for this cache
   * 
   * @param cache
   *          the cache instance
   * @param managerURI
   *          uri of the cache manager
   * @param cacheName
   *          cache name/alias
   * @param domain
   *          domain of the bean
   * @return
   * @throws MalformedObjectNameException
   */
  public static ObjectName createObjectName(URI managerURI, String cacheName,
      String domain) throws MalformedObjectNameException {
    return new ObjectName(domain + ":type=CacheStatistics,CacheManager="
        + managerURI.toASCIIString() + ",Cache=" + cacheName);
  }

  /**
   * Create MBean object name for this cache. Domain of the mbean is the default
   * 'org.ehcache'
   * 
   * @param cache
   *          the cache instance
   * @param managerURI
   *          uri of the cache manager
   * @param cacheName
   *          cache name/alias
   * @return
   * @throws MalformedObjectNameException
   */
  public static ObjectName createObjectName(URI managerURI, String cacheName)
      throws MalformedObjectNameException {
    return createObjectName(managerURI, cacheName, DEFAULT_MBEAN_DOMAIN);
  }

  /**
   * Register your cache for statistics monitoring via JMX bean
   * 
   * The mbean domain will be the default org.ehcache
   * 
   * @param cache
   *          the cache instance
   * @param managerURI
   *          uri of the cache manager
   * @param cacheName
   *          cache name/alias
   * @throws InstanceAlreadyExistsException
   *           if you're registering for the same cache more than one
   * @throws MBeanRegistrationException
   *           The preRegister (MBeanRegistration interface) method of the MBean
   *           has thrown an exception. The MBean will not be registered.
   * @throws MalformedObjectNameException
   *           if cacheName doesn't match with ObjectName guideline
   * 
   */
  public static ObjectName register(Cache<?, ?> cache, URI managerURI, String cacheName)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      MalformedObjectNameException {
    return register(cache, managerURI, cacheName, DEFAULT_MBEAN_DOMAIN);
  }

  /**
   * Register your cache for statistics monitoring via JMX bean, specifying
   * cache name and domain name
   * 
   * @param cache
   *          the cache instance
   * @param managerURI
   *          uri of the cache manager
   * @param cacheName
   *          cache name/alias
   * @param domain
   *          domain of the bean
   * @throws InstanceAlreadyExistsException
   *           if you're registering for the same cache more than one
   * @throws MBeanRegistrationException
   *           The preRegister (MBeanRegistration interface) method of the MBean
   *           has thrown an exception. The MBean will not be registered.
   * @throws MalformedObjectNameException
   *           if cacheName doesn't match with ObjectName guideline
   * 
   */
  public static ObjectName register(Cache<?, ?> cache, URI managerURI, String cacheName,
      String domain) throws InstanceAlreadyExistsException, MBeanRegistrationException,
      MalformedObjectNameException {
    try {
      ObjectName mBeanName = createObjectName(managerURI, cacheName, domain);
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      CacheStatisticsMXBean statisticsMXBean = new CacheStatisticsMXBeanImpl(cache);
      mbs.registerMBean(statisticsMXBean, mBeanName);
      return mBeanName;
    } catch (NotCompliantMBeanException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Unregister statistics mbean, using default org.ehcache domain
   * 
   * @param cache
   *          the cache instance
   * @param managerURI
   *          uri of the cache manager
   * @param cacheName
   *          cache name/alias
   * @throws MBeanRegistrationException
   * @throws MalformedObjectNameException 
   */
  public static void unregister(URI managerURI, String cacheName)
      throws MBeanRegistrationException, MalformedObjectNameException {
    unregister(managerURI, cacheName, DEFAULT_MBEAN_DOMAIN);
  }

  /**
   * Unregister statistics mbean
   * 
   * @param cache
   *          the cache instance
   * @param managerURI
   *          uri of the cache manager
   * @param cacheName
   *          cache name/alias
   * @param domain
   *          domain of the bean
   * @throws MBeanRegistrationException
   * @throws MalformedObjectNameException 
   */
  public static void unregister(URI managerURI, String cacheName, String domain)
      throws MBeanRegistrationException, MalformedObjectNameException {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    try {
      mbs.unregisterMBean(createObjectName(managerURI, cacheName, domain));
    } catch (InstanceNotFoundException e) {
      // don't care
    }
  }
}
