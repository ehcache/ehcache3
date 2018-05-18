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
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.xml.model.CacheDefinition;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.TimeType;
import org.ehcache.xml.model.TimeUnit;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.pany.ehcache.MyExpiry;
import com.pany.ehcache.integration.TestEvictionAdvisor;

import java.math.BigInteger;
import java.net.URL;
import java.time.Duration;
import java.util.stream.StreamSupport;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class CoreCacheConfigurationParserTest {

  ClassLoader classLoader = this.getClass().getClassLoader();
  CacheConfigurationBuilder<Object, Object> cacheConfigurationBuilder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10));
  CoreCacheConfigurationParser parser = new CoreCacheConfigurationParser();

  @Test
  public void parseConfigurationExpiryPolicy() throws Exception {
    ConfigurationParser rootParser = parseXmlConfiguration("/configs/expiry-caches.xml");

    ExpiryPolicy<?, ?> expiry = getCacheDefinitionFrom(rootParser, "none").getExpiryPolicy();
    ExpiryPolicy<?, ?> value = ExpiryPolicyBuilder.noExpiration();
    assertThat(expiry, is(value));

    expiry = getCacheDefinitionFrom(rootParser, "notSet").getExpiryPolicy();
    value = ExpiryPolicyBuilder.noExpiration();
    assertThat(expiry, is(value));

    expiry = getCacheDefinitionFrom(rootParser, "class").getExpiryPolicy();
    assertThat(expiry, CoreMatchers.instanceOf(com.pany.ehcache.MyExpiry.class));

    expiry = getCacheDefinitionFrom(rootParser, "deprecatedClass").getExpiryPolicy();
    assertThat(expiry.getExpiryForCreation(null, null), is(Duration.ofSeconds(42)));
    assertThat(expiry.getExpiryForAccess(null, () -> null), is(Duration.ofSeconds(42)));
    assertThat(expiry.getExpiryForUpdate(null, () -> null, null), is(Duration.ofSeconds(42)));

    expiry = getCacheDefinitionFrom(rootParser, "tti").getExpiryPolicy();
    value = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(500));
    assertThat(expiry, equalTo(value));

    expiry = getCacheDefinitionFrom(rootParser, "ttl").getExpiryPolicy();
    value = ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(30));
    assertThat(expiry, equalTo(value));
  }

  private ConfigurationParser parseXmlConfiguration(String resourcePath) throws Exception {
    URL resource = this.getClass().getResource(resourcePath);
    return new ConfigurationParser(resource.toExternalForm());
  }

  private CacheConfiguration<?,?> getCacheDefinitionFrom(ConfigurationParser rootParser, String cacheName) throws Exception {
    CacheDefinition cacheDefinition = StreamSupport.stream(rootParser.getCacheElements().spliterator(), false)
      .filter(def -> def.id().equals(cacheName)).findAny().get();
    return parser.parseConfiguration(cacheDefinition, classLoader, cacheConfigurationBuilder).build();
  }

  @Test
  public void unparseConfigurationNoExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(ExpiryPolicyBuilder.noExpiration());
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    assertThat(cacheType.getExpiry().getNone(), notNullValue());
  }

  @Test
  public void unparseConfigurationCustomExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(new MyExpiry());
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    assertThat(cacheType.getExpiry().getClazz(), is(MyExpiry.class.getName()));
  }

  @Test
  public void unparseConfigurationTtiExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofSeconds(5)));
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    TimeType tti = cacheType.getExpiry().getTti();
    assertThat(tti, notNullValue());
    assertThat(tti.getValue(), is(BigInteger.valueOf(5)));
    assertThat(tti.getUnit(), is(TimeUnit.SECONDS));
  }

  @Test
  public void unparseConfigurationTtlExpiry() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(5)));
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    TimeType ttl = cacheType.getExpiry().getTtl();
    assertThat(ttl, notNullValue());
    assertThat(ttl.getValue(), is(BigInteger.valueOf(5)));
    assertThat(ttl.getUnit(), is(TimeUnit.SECONDS));
  }

  @Test
  public void unparseConfigurationEvictionAdvisor() {
    CacheConfiguration<Object, Object> cacheConfiguration = buildCacheConfigWith(new TestEvictionAdvisor<>());
    CacheType cacheType = parser.unparseConfiguration(cacheConfiguration, new CacheType());
    assertThat(cacheType.getEvictionAdvisor(), is(TestEvictionAdvisor.class.getName()));
  }

  private CacheConfiguration<Object, Object> buildCacheConfigWith(ExpiryPolicy<Object, Object> expiryPolicy) {
    return cacheConfigurationBuilder.withExpiry(expiryPolicy).build();
  }

  private CacheConfiguration<Object, Object> buildCacheConfigWith(EvictionAdvisor<Object, Object> evictionAdvisor) {
    return cacheConfigurationBuilder.withEvictionAdvisor(new TestEvictionAdvisor<>()).build();
  }
}
