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
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.Expiry;
import org.ehcache.xml.model.ExpiryType;
import org.ehcache.xml.model.TimeType;
import org.ehcache.xml.model.TimeUnit;

import java.math.BigInteger;
import java.time.Duration;

import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;
import static org.ehcache.xml.XmlConfiguration.getInstanceOfName;

public class CoreCacheConfigurationParser {

  public <K, V> CacheConfigurationBuilder<K, V> parseConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                   CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    final Expiry parsedExpiry = cacheDefinition.expiry();
    if (parsedExpiry != null) {
      cacheBuilder = cacheBuilder.withExpiry(getExpiry(cacheClassLoader, parsedExpiry));
    }

    EvictionAdvisor evictionAdvisor = getInstanceOfName(cacheDefinition.evictionAdvisor(), cacheClassLoader, EvictionAdvisor.class);
    cacheBuilder = cacheBuilder.withEvictionAdvisor(evictionAdvisor);

    return cacheBuilder;
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private static ExpiryPolicy<? super Object, ? super Object> getExpiry(ClassLoader cacheClassLoader, Expiry parsedExpiry)
    throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final ExpiryPolicy<? super Object, ? super Object> expiry;
    if (parsedExpiry.isUserDef()) {
      ExpiryPolicy<? super Object, ? super Object> tmpExpiry;
      try {
        tmpExpiry = getInstanceOfName(parsedExpiry.type(), cacheClassLoader, ExpiryPolicy.class);
      } catch (ClassCastException e) {
        tmpExpiry = ExpiryUtils.convertToExpiryPolicy(getInstanceOfName(parsedExpiry.type(), cacheClassLoader, org.ehcache.expiry.Expiry.class));
      }
      expiry = tmpExpiry;
    } else if (parsedExpiry.isTTL()) {
      expiry = ExpiryPolicyBuilder.timeToLiveExpiration(Duration.of(parsedExpiry.value(), parsedExpiry.unit()));
    } else if (parsedExpiry.isTTI()) {
      expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.of(parsedExpiry.value(), parsedExpiry.unit()));
    } else {
      expiry = ExpiryPolicyBuilder.noExpiration();
    }
    return expiry;
  }

  public CacheType unparseConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    ExpiryPolicy<?, ?> expiryPolicy = cacheConfiguration.getExpiryPolicy();
    if (expiryPolicy != null) {
      ExpiryType expiryType = new ExpiryType();
      if (expiryPolicy == ExpiryPolicy.NO_EXPIRY) {
        expiryType.withNone(new ExpiryType.None());
      } else if (expiryPolicy instanceof ExpiryPolicyBuilder.TimeToLiveExpiryPolicy) {
        Duration ttl = expiryPolicy.getExpiryForCreation(null, null);
        expiryType.withTtl(new TimeType().withValue(BigInteger.valueOf(ttl.getSeconds())).withUnit(TimeUnit.SECONDS));
      } else if (expiryPolicy instanceof ExpiryPolicyBuilder.TimeToIdleExpiryPolicy) {
        Duration tti = expiryPolicy.getExpiryForCreation(null, null);
        expiryType.withTti(new TimeType().withValue(BigInteger.valueOf(tti.getSeconds())).withUnit(TimeUnit.SECONDS));
      } else {
        expiryType.setClazz(expiryPolicy.getClass().getName());
      }
      cacheType.withExpiry(expiryType);
    }

    EvictionAdvisor<?, ?> evictionAdvisor = cacheConfiguration.getEvictionAdvisor();
    if (evictionAdvisor != null) {
      cacheType.withEvictionAdvisor(evictionAdvisor.getClass().getName());
    }

    return cacheType;
  }
}
