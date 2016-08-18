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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import javax.cache.CacheException;

import org.junit.Before;
import org.junit.Test;

public class DefaultConfigurationResolverTest {

  private static URI makeURI() throws URISyntaxException {
    return new URI("cheese://" + System.nanoTime());
  }

  @Before
  public void setUp() {
    // just in case to prevent cross talk between test methods
    System.getProperties().remove(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME);
  }

  @Test
  public void testCacheManagerProps() throws Exception {
    URI uri = makeURI();

    Properties props = new Properties();
    props.put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, uri);

    URI resolved = DefaultConfigurationResolver.resolveConfigURI(props);
    assertSame(uri, resolved);
  }

  @Test
  public void testSystemProperty() throws Exception {
    URI uri = makeURI();

    System.getProperties().put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, uri);

    URI resolved = DefaultConfigurationResolver.resolveConfigURI(new Properties());
    assertSame(uri, resolved);
  }

  @Test
  public void testCacheManagerPropertiesOverridesSystemProperty() throws Exception {
    URI uri1 = makeURI();
    URI uri2 = makeURI();

    assertFalse(uri1.equals(uri2));

    Properties props = new Properties();
    props.put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, uri1);
    System.getProperties().put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, uri2);

    URI resolved = DefaultConfigurationResolver.resolveConfigURI(props);
    assertSame(uri1, resolved);
  }

  @Test
  public void testDefault() throws Exception {
    URI resolved = DefaultConfigurationResolver.resolveConfigURI(new Properties());
    assertNull(resolved);
  }

  @Test
  public void testURL() throws Exception {
    URL url = new URL("http://www.cheese.com/asiago");

    Properties props = new Properties();
    props.put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, url);

    URI resolved = DefaultConfigurationResolver.resolveConfigURI(props);
    assertEquals(url.toURI(), resolved);
  }

  @Test
  public void testString() throws Exception {
    String string = "http://www.cheese.com/armenian-string-cheese/";

    Properties props = new Properties();
    props.put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, string);

    URI resolved = DefaultConfigurationResolver.resolveConfigURI(props);
    assertEquals(new URI(string), resolved);
  }

  @Test
  public void testInvalidType() throws Exception {
    Properties props = new Properties();
    props.put(DefaultConfigurationResolver.DEFAULT_CONFIG_PROPERTY_NAME, this);

    try {
      DefaultConfigurationResolver.resolveConfigURI(props);
      fail();
    } catch (CacheException ce) {
      // expected
    }
  }

}
