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
package org.ehcache.testing;

import org.terracotta.ipceventbus.proc.AnyProcess;
import org.terracotta.ipceventbus.proc.AnyProcessBuilder;
import org.terracotta.testing.config.DefaultStartupCommandBuilder;
import org.terracotta.testing.config.StartupCommandBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.terracotta.testing.config.ConfigConstants.DEFAULT_CLUSTER_NAME;

public class DynamicConfigStartupBuilder extends DefaultStartupCommandBuilder {
  private int stripeId;
  private String[] builtCommand;
  private int debugPort = Integer.getInteger("serverDebugPortStart", 0);

  @Override
  public StartupCommandBuilder stripeName(String stripeName) {
    this.stripeId = Integer.parseInt(stripeName.substring(6));
    return this;
  }

  @Override
  public String[] build() {
    if (builtCommand == null) {
      try {
        installServer();
        Path generatedRepositories = convertConfigFiles();
        // moves the generated files onto the server folder, but only for this server we are building
        Files.move(generatedRepositories.resolve("stripe-" + stripeId).resolve(getServerName()), getServerWorkingDir().resolve("repository"));
        buildStartupCommand();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return builtCommand;
  }

  private void buildStartupCommand() {
    List<String> command = new ArrayList<>();
    String scriptPath = getAbsolutePath(Paths.get("server", "bin", "start-tc-server"));
    command.add(scriptPath);
    if (isConsistentStartup()) {
      command.add("-c");
    }
    command.add("-r");
    command.add("repository");
    builtCommand = command.toArray(new String[0]);
  }

  private Path convertConfigFiles() {
    Path generatedRepositories = getServerWorkingDir().getParent().getParent().resolve("generated-repositories");

    if (Files.exists(generatedRepositories)) {
      // this builder is called fro each server, but the CLI will generate the repositories for all.
      return generatedRepositories;
    }

    List<String> command = new ArrayList<>();
    String scriptPath = getAbsolutePath(Paths.get("tools", "config-convertor", "bin", "config-convertor"));
    command.add(scriptPath);
    command.add("convert");

    command.add("-c");
    command.add(getTcConfig().toString());

    command.add("-n");
    command.add(DEFAULT_CLUSTER_NAME);

    command.add("-d");
    command.add(getServerWorkingDir().relativize(generatedRepositories).toString());

    command.add("-f"); //Do not fail for relative paths

    AnyProcess process;
    try {
      AnyProcessBuilder<? extends AnyProcess> builder = AnyProcess.newBuilder()
          .command(command.toArray(new String[0]))
          .workingDir(getServerWorkingDir().toFile())
          .pipeStdout(System.out)
          .pipeStderr(System.err);
      if (debugPort > 0) {
        builder.env("JAVA_OPTS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + debugPort);
      }
      process = builder.build();
    } catch (Exception e) {
      throw new IllegalStateException("Error launching command: " + String.join(" ", command), e);
    }
    int exitStatus;
    try {
      exitStatus = process.waitFor();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    if (exitStatus != 0) {
      throw new IllegalStateException("Process: " + process.getCommand() + " exited with status: " + exitStatus);
    }
    return generatedRepositories;
  }
}
