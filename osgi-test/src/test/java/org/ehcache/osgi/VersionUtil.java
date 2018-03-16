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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Used to retrieve version of dependencies. Made to work with Gradle and in an IDE.
 */
public final class VersionUtil {

  private static Properties properties;

  private VersionUtil() {
  }

  private static Properties get() {
    // No need to support multithreading
    if(properties == null) {
      properties = new Properties();
      try {
        properties.load(Files.newBufferedReader(Paths.get("../gradle.properties"), StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return properties;
  }

  public static String version(String systemProperty, String gradleProperty) {
    String version = System.getProperty(systemProperty);
    if(version != null) {
      return version;
    }
    return get().getProperty(gradleProperty);
  }

  public static String ehcacheOsgiJar() {
    String version = System.getProperty("ehcache.osgi.jar");
    if(version != null) {
      return version;
    }
    return "../dist/build/libs/ehcache-" + properties.getProperty("ehcacheVersion") + ".jar";
  }
}
