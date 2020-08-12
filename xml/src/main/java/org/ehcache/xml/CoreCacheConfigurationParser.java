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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.Expiry;
import org.ehcache.xml.model.ExpiryType;
import org.ehcache.xml.model.TimeType;
import org.ehcache.xml.model.TimeUnit;

import java.math.BigInteger;
import java.time.Duration;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.core.config.ExpiryUtils.jucTimeUnitToTemporalUnit;
import static org.ehcache.xml.XmlConfiguration.getClassForName;
import static org.ehcache.xml.XmlModel.convertToXmlTimeUnit;

public class CoreCacheConfigurationParser {

  public <K, V> CacheConfigurationBuilder<K, V> parseConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                   CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    final Expiry parsedExpiry = cacheDefinition.expiry();
    if (parsedExpiry != null) {
      cacheBuilder = cacheBuilder.withExpiry(getExpiry(cacheClassLoader, parsedExpiry));
    }

    @SuppressWarnings("unchecked")
    EvictionAdvisor<? super K, ? super V> evictionAdvisor = getInstanceOfName(cacheDefinition.evictionAdvisor(), cacheClassLoader, EvictionAdvisor.class);
    cacheBuilder = cacheBuilder.withEvictionAdvisor(evictionAdvisor);

    return cacheBuilder;
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private static ExpiryPolicy<? super Object, ? super Object> getExpiry(ClassLoader cacheClassLoader, Expiry parsedExpiry)
    throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (parsedExpiry.isUserDef()) {
      try {
        return getInstanceOfName(parsedExpiry.type(), cacheClassLoader, ExpiryPolicy.class);
      } catch (ClassCastException e) {
        return ExpiryUtils.convertToExpiryPolicy(getInstanceOfName(parsedExpiry.type(), cacheClassLoader, org.ehcache.expiry.Expiry.class));
      }
    } else if (parsedExpiry.isTTL()) {
      return ExpiryPolicyBuilder.timeToLiveExpiration(Duration.of(parsedExpiry.value(), parsedExpiry.unit()));
    } else if (parsedExpiry.isTTI()) {
      return ExpiryPolicyBuilder.timeToIdleExpiration(Duration.of(parsedExpiry.value(), parsedExpiry.unit()));
    } else {
      return ExpiryPolicyBuilder.noExpiration();
    }
  }

  static <T> T getInstanceOfName(String name, ClassLoader classLoader, Class<T> type) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (name == null) {
      return null;
    }
    Class<?> klazz = getClassForName(name, classLoader);
    return klazz.asSubclass(type).newInstance();
  }

  public CacheType unparseConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    ExpiryPolicy<?, ?> expiryPolicy = cacheConfiguration.getExpiryPolicy();
    if (expiryPolicy != null) {
      Duration expiry = expiryPolicy.getExpiryForCreation(null, null);
      ExpiryType expiryType = new ExpiryType();
      if (expiryPolicy.equals(ExpiryPolicy.NO_EXPIRY)) {
        expiryType.withNone(new ExpiryType.None());
      } else if (expiryPolicy.equals(ExpiryPolicyBuilder.timeToLiveExpiration(expiry))) {
        expiryType.withTtl(convertToTimeType(expiry));
      } else if (expiryPolicy.equals(ExpiryPolicyBuilder.timeToIdleExpiration(expiry))) {
        expiryType.withTti(convertToTimeType(expiry));
      } else {
        throw new XmlConfigurationException("XML translation of custom expiry policy is not supported");
      }
      cacheType.withExpiry(expiryType);
    }

    EvictionAdvisor<?, ?> evictionAdvisor = cacheConfiguration.getEvictionAdvisor();
    if (evictionAdvisor != null) {
      throw new XmlConfigurationException("XML translation of eviction advisor is not supported");
    }

    return cacheType;
  }

  private static TimeType convertToTimeType(Duration duration) {
    return Stream.of(java.util.concurrent.TimeUnit.values())
      .sorted(comparing(unit -> unit.convert(duration.toNanos(), NANOSECONDS)))
      .filter(unit -> duration.equals(Duration.of(unit.convert(duration.toNanos(), NANOSECONDS), jucTimeUnitToTemporalUnit(unit))))
      .findFirst()
      .map(unit -> new TimeType()
        .withValue(BigInteger.valueOf(unit.convert(duration.toNanos(), NANOSECONDS)))
        .withUnit(convertToXmlTimeUnit(unit))
      ).orElseThrow(AssertionError::new);
  }
}
