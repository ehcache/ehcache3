/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.testing;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ConfigRepoStartupBuilder extends ConfigFileStartupBuilder {
  private String[] builtCommand;

  public ConfigRepoStartupBuilder() {
  }

  @Override
  public String[] build() {
    if (builtCommand == null) {
      try {
        Path tcConfig = installServer();
        Path generatedRepositories = convertToConfigFile(tcConfig, false);

        // moves the generated files onto the server folder, but only for this server we are building
        Path source = generatedRepositories.resolve("stripe-" + getStripeId()).resolve(getServerName()).toAbsolutePath();
        Path destination = getServerWorkingDir().resolve("config").toAbsolutePath();
        org.terracotta.utilities.io.Files.relocate(source, destination);
        buildStartupCommand(destination);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return builtCommand.clone();
  }

  private void buildStartupCommand(Path destination) {
    List<String> command = new ArrayList<>();

    if (isConsistentStartup()) {
      command.add("-c");
    }

    command.add("-r");
    command.add(destination.toAbsolutePath().toString());
    builtCommand = command.toArray(new String[0]);
  }
}
