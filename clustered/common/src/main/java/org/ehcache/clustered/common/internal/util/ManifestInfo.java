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
package org.ehcache.clustered.common.internal.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public class ManifestInfo {

  public static final String VERSION = "Bundle-Version";
  public static final String BUILD_TIMESTAMP = "BuildInfo-Timestamp";
  public static final String BUILD_JDK = "Build-Jdk";

  public static <T> Map<String, String> getJarManifestInfo(Class<T> cls) {
    try {
      File file = new File(cls.getProtectionDomain().getCodeSource().getLocation().toURI());
      if (file.isDirectory()) {
        return Collections.emptyMap();
      }
      try (JarFile jar = new JarFile(file)) {
        return jar.getManifest().getMainAttributes().entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
      }
    } catch (IOException | URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }
}
