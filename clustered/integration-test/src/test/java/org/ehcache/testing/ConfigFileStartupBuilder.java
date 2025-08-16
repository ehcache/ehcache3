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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.terracotta.dynamic_config.cli.upgrade_tools.config_converter.ConfigConverterTool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.terracotta.testing.config.ConfigConstants;
import org.terracotta.testing.config.DefaultStartupCommandBuilder;
import org.terracotta.testing.config.StartupCommandBuilder;

public class ConfigFileStartupBuilder extends DefaultStartupCommandBuilder {
  private String[] builtCommand;
  private int stripeId;
  private String clusterName = ConfigConstants.DEFAULT_CLUSTER_NAME;

  public ConfigFileStartupBuilder() {
  }

  @Override
  public StartupCommandBuilder stripeName(String stripeName) {
    super.stripeName(stripeName);
    this.stripeId = Integer.parseInt(stripeName.substring(6));
    return this;
  }

  public int getStripeId() {
    return stripeId;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public String[] build() {
    if (builtCommand == null) {
      try {
        Path tcConfig = installServer();
        Path configDir = convertToConfigFile(tcConfig, true).resolve("test.properties");
        builtCommand = configFileStartupCommand(configDir);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return builtCommand.clone();
  }

  private String[] configFileStartupCommand(Path configFile) {
    List<String> command = new ArrayList<>();

    command.add("-f");
    command.add(configFile.toString());

    command.add("-n");
    command.add(getServerName());

    command.add("--auto-activate");

    Path configDir;
    try {
      configDir = Files.createTempDirectory(getServerWorkingDir(), "config");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    command.add("-r");
    command.add(configDir.toString());

    return command.toArray(String[]::new);
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  protected Path convertToConfigFile(Path tcConfig, boolean properties) {
    Path generatedConfigFileDir = getServerWorkingDir().getParent().resolve("generated-configs");

    if (Files.exists(generatedConfigFileDir)) {
      // this builder is called for each server, but the CLI will generate the config directories for all.
      return generatedConfigFileDir;
    }

    List<String> command = new ArrayList<>();
    command.add("convert");

    command.add("-c");
    command.add(tcConfig.toString());

    for (int i = 0; i < 1; i++) {
      command.add("-s");
      command.add("stripe[" + i + "]");
    }
    if (properties) {
      command.add("-t");
      command.add("properties");
    }

    command.add("-d");
    command.add(generatedConfigFileDir.toString());

    command.add("-f"); //Do not fail for relative paths

    command.add("-n");
    command.add(getClusterName());

    executeCommand(command);
    return generatedConfigFileDir;
  }

  protected static void executeCommand(List<String> command) {
    new ConfigConverterTool().run(command.toArray(String[]::new));
  }
}
