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

import com.tc.productinfo.ExtensionInfo;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;

/**
 * @author Mathieu Carbou
 */
public abstract class ExtensionInfoSkeleton implements ExtensionInfo {

  public enum Type {
    CONFIG,
    PLUGIN,
    SERVICE,
    ENTITY
  }

  @Override
  public String getExtensionInfo() {
    return getValue(DESCRIPTION);
  }

  @Override
  public String getValue(String name) {
    switch (name) {
      case "type":
        return getType().name();
      case NAME:
        return getName();
      case "version":
        return getJarManifestInfo().getValue("Bundle-Version");
      case "timestamp":
        return getJarManifestInfo().getValue("BuildInfo-Timestamp");
      case "jdk":
        return getJarManifestInfo().getValue("Built-JDK");
      case DESCRIPTION:
        return String.format(" * %-35s %-15s (built on %s with JDK %s)",
          getFormattedName(),
          getValue("version"),
          getValue("timestamp"),
          getValue("jdk"));
      default:
        return getJarManifestInfo().getValue(name);
    }
  }

  protected abstract String getName();

  protected abstract Type getType();

  protected Attributes getJarManifestInfo() {
    try {
      File file = new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
      if (file.isDirectory()) {
        return new Attributes(0);
      }
      try (JarFile jar = new JarFile(file)) {
        return jar.getManifest().getMainAttributes();
      }
    } catch (IOException | URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  protected String getFormattedName() {
    switch (getType()) {
      case ENTITY:
        return "Entity : " + getName();
      case SERVICE:
        return "Service: " + getName();
      case PLUGIN:
        return "Plugin : " + getName();
      case CONFIG:
        return "Config : " + getName();
      default:
        return "Unknown: " + getName();
    }
  }
}
