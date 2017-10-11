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
package org.ehcache.clustered;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Base class for all clustered tests. It makes sure the environment is correctly configured to launch the servers. Especially
 * in the IDE.
 */
public abstract class ClusteredTests {

  private static final boolean FORCE_KIT_REFRESH = false;

  static {
    initInstallationPath();
  }

  private static void initInstallationPath() {
    if(System.getProperty("kitInstallationPath") != null) {
      return; // nothing to do, all set
    }

    String currentDir = System.getProperty("user.dir");

    // We might have the root of ehcache or in the integration-test directory
    // as current working directory
    String diskPrefix;
    if(Paths.get(currentDir).getFileName().toString().equals("integration-test")) {
      diskPrefix = "";
    }
    else {
      diskPrefix = "clustered/integration-test/";
    }

    String kitInstallationPath = getKitInstallationPath(diskPrefix);

    if (kitInstallationPath == null || FORCE_KIT_REFRESH) {
      installKit(diskPrefix);
      kitInstallationPath = getKitInstallationPath(diskPrefix);
    }

    System.setProperty("kitInstallationPath", kitInstallationPath);
  }

  private static void installKit(String diskPrefix) {
    try {
      Process process = new ProcessBuilder(diskPrefix + "../../gradlew", "copyServerLibs")
        .redirectError(ProcessBuilder.Redirect.INHERIT)
        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
        .start();
      int status = process.waitFor();
      assertThat(status).isZero();
    } catch (IOException e) {
      fail("Failed to start gradle to install kit", e);
    } catch (InterruptedException e) {
      fail("Interrupted while installing kit", e);
    }
  }

  private static String getKitInstallationPath(String diskPrefix) {
    String basedir = diskPrefix + "build/ehcache-kit";
    if(!new File(basedir).exists()) {
      return null;
    }
    try {
      return Files.list(Paths.get(basedir))
        .sorted(Comparator.<Path>naturalOrder().reversed()) // the last one should be the one with the highest version
        .findFirst()
        .map(path -> path.toAbsolutePath().normalize().toString())
        .orElse(null);
    } catch (IOException e) {
      fail("Failed to set kitInstallationPath from " + basedir, e);
      return null;
    }
  }
}
