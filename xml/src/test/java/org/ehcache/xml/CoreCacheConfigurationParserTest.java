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
import org.ehcache.config.Configuration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.TimeType;
import org.ehcache.xml.model.TimeUnit;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.pany.ehcache.MyExpiry;
import com.pany.ehcache.integration.TestEvictionAdvisor;

import java.math.BigInteger;
import java.time.Duration;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class CoreCacheConfigurationParserTest {

  CacheConfigurationBuilder<Object, Object> cacheConfigurationBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10));
  CoreCacheConfigurationParser parser = new CoreCacheConfigurationParser();

  @Test
  public void parseConfigurationExpiryPolicy() throws Exception {
    Configuration configuration = new XmlConfiguration(getClass().getResource("/configs/expiry-caches.xml"));

    ExpiryPolicy<?, ?> expiry = configuration.getCacheConfigurations().get("none").getExpiryPolicy();
    ExpiryPolicy<?, ?> value = ExpiryPolicyBuilder.noExpiration();
    assertThat(expiry, is(value));

    expiry = configuration.getCacheConfigurations().get("notSet").getExpiryPolicy();
    value = ExpiryPolicyBuilder.noExpiration();
    assertThat(expiry, is(value));

    expiry = configuration.getCacheConfigurations().get("class").getExpiryPolicy();
    assertThat(expiry, CoreMatchers.instanceOf(com.pany.ehcache.MyExpiry.class));

    expiry = configuration.getCacheConfigurations().get("deprecatedClass").getExpiryPolicy();
    assertThat(expiry.getExpiryForCreation(null, null), is(Duration.ofSeconds(42)));
    assertThat(expiry.getExpiryForAccess(null, () -> null), is(Duration.ofSeconds(42)));
    assertThat(expiry.getExpiryForUpdate(null, () -> null, null), is(Duration.ofSeconds(42)));

    expiry = configuration.getCacheConfigurations().get("tti").getExpiryPolicy();
    value = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(500));
    assertThat(expiry, equalTo(value));

    expiry = configuration.getCacheConfigurations().get("ttl").getExpiryPolicy();
    value = ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(30));
    assertThat(expiry, equalTo(value));
  }

  @Test
  public void unparseConfigurationNoExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(ExpiryPolicyBuilder.noExpiration());
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    assertThat(cacheType.getExpiry().getNone(), notNullValue());
  }

  @Test(expected = XmlConfigurationException.class)
  public void unparseConfigurationCustomExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(new MyExpiry());
    parser.unparseConfiguration(cacheConfiguration, new CacheType());
  }

  @Test
  public void unparseConfigurationTtiExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(2500)));
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    TimeType tti = cacheType.getExpiry().getTti();
    assertThat(tti, notNullValue());
    assertThat(tti.getValue(), is(BigInteger.valueOf(2500)));
    assertThat(tti.getUnit(), is(TimeUnit.MILLIS));
  }

  @Test
  public void unparseConfigurationTtlExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMinutes(60)));
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    TimeType ttl = cacheType.getExpiry().getTtl();
    assertThat(ttl, notNullValue());
    assertThat(ttl.getValue(), is(BigInteger.valueOf(1)));
    assertThat(ttl.getUnit(), is(TimeUnit.HOURS));
  }

  @Test(expected = XmlConfigurationException.class)
  public void unparseConfigurationEvictionAdvisor() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(new TestEvictionAdvisor<>());
    parser.unparseConfiguration(cacheConfiguration, new CacheType());
  }

  private CacheConfiguration<Object, Object> buildCacheConfigWith(ExpiryPolicy<Object, Object> expiryPolicy) {
    return cacheConfigurationBuilder.withExpiry(expiryPolicy).build();
  }

  private CacheConfiguration<Object, Object> buildCacheConfigWith(EvictionAdvisor<Object, Object> evictionAdvisor) {
    return cacheConfigurationBuilder.withEvictionAdvisor(evictionAdvisor).build();
  }
}
