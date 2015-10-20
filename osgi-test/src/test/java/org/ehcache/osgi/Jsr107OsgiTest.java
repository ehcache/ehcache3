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

package org.ehcache.osgi;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * Jsr107OsgiTest
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class Jsr107OsgiTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Configuration
  public Option[] config() {
    return options(
        mavenBundle("org.slf4j", "slf4j-api", "1.7.7"),
        mavenBundle("org.slf4j", "slf4j-simple", "1.7.7").noStart(),
        mavenBundle("org.ehcache", "ehcache", "3.0.0-SNAPSHOT"),
        mavenBundle("javax.cache", "cache-api", "1.0.0"),
        mavenBundle("org.terracotta", "management-model", "0.2.0"),
        junitBundles()
    );
  }

  @Test
  @Ignore("Needs https://github.com/jsr107/jsr107spec/issues/326 to be fixed and so will wait on javax.cache:cache-api:1.0.1 only")
  public void testJsr107EhcacheOsgi() throws Exception {
    File configFile = createConfigFile();

    CachingProvider cachingProvider = Caching.getCachingProvider("org.ehcache.jsr107.EhcacheCachingProvider", getClass().getClassLoader());
    CacheManager cacheManager = cachingProvider.getCacheManager(configFile.toURI(), getClass().getClassLoader());
  }

  private File createConfigFile() throws IOException {
    File configFile = temporaryFolder.newFile("ehcache-107.xml");

    PrintWriter writer = new PrintWriter(configFile);
    writer.println("<config\n" +
                   "    xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" +
                   "    xmlns='http://www.ehcache.org/v3'\n" +
                   "    xmlns:jsr107='http://www.ehcache.org/v3/jsr107'\n" +
                   "    xsi:schemaLocation=\"http://www.ehcache.org/v3 ../../../../xml/src/main/resources/ehcache-core.xsd\n" +
                   "                        http://www.ehcache.org/v3/jsr107 ../../main/resources/ehcache-107ext.xsd\">\n" +
                   "\n" +
                   "  <service>\n" +
                   "    <jsr107:defaults default-template=\"tinyCache\">\n" +
                   "      <jsr107:cache name=\"foos\" template=\"stringCache\"/>\n" +
                   "    </jsr107:defaults>\n" +
                   "  </service>\n" +
                   "\n" +
                   "  <cache-template name=\"stringCache\">\n" +
                   "    <key-type>java.lang.String</key-type>\n" +
                   "    <value-type>java.lang.String</value-type>\n" +
                   "    <heap size=\"2000\" unit=\"entries\"/>\n" +
                   "  </cache-template>\n" +
                   "\n" +
                   "  <cache-template name=\"tinyCache\">\n" +
                   "    <heap size=\"20\" unit=\"entries\"/>\n" +
                   "  </cache-template>\n" +
                   "\n" +
                   "</config>");
    writer.close();

    return configFile;
  }

}
