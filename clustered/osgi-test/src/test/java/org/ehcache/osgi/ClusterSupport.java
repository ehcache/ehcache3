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

package org.ehcache.osgi;

import org.terracotta.utilities.test.net.PortManager;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public class ClusterSupport {

  public static Cluster startServer(Path serverDirectory) throws IOException {
    Path kitLocation = Paths.get(System.getProperty("kitInstallationPath"));

    PortManager portManager = PortManager.getInstance();
    PortManager.PortRef tsaPort = portManager.reservePort();
    PortManager.PortRef tsaGroupPort = portManager.reservePort();

    Path serverDir = kitLocation.resolve("server");

    ProcessBuilder serverProcess = new ProcessBuilder()
      .directory(serverDirectory.toFile())
      .command(Paths.get(System.getProperty("java.home")).resolve("bin")
        .resolve(System.getProperty("os.name").contains("Windows") ? "java.exe" : "java").toString());

    serverProcess.command().addAll(asList(
      "-Xmx128m",
      "-Dtc.install-root=" + serverDir,
      "-cp", serverDir.resolve("lib").resolve("tc.jar").toString(),
      "com.tc.server.TCServerMain",
      "--auto-activate",
      "--cluster-name=foo",
      "--failover-priority=availability",
      "--client-reconnect-window=120s",
      "--name=default-server",
      "--hostname=localhost",
      "--port=" + tsaPort.port(),
      "--group-port=" + tsaGroupPort.port(),
      "--log-dir=" + serverDirectory.resolve("logs"),
      "--config-dir=" + serverDirectory.resolve("repository"),
      "--offheap-resources=main:32MB"));
    serverProcess.inheritIO();

    return new Cluster(serverProcess.start(), URI.create("terracotta://localhost:" + tsaPort.port()), serverDirectory, tsaPort, tsaGroupPort);
  }

  private static int selectAvailableEphemeralPort() throws IOException {
    try (ServerSocketChannel channel = ServerSocketChannel.open().bind(new InetSocketAddress(0))) {
      return channel.socket().getLocalPort();
    }
  }

  static class Cluster implements Closeable {

    private final Process serverProcess;
    private final URI connectionUri;
    private final Path workingPath;
    private final Collection<PortManager.PortRef> ports;

    Cluster(Process serverProcess, URI connectionUri, Path workingPath, PortManager.PortRef... ports) {
      this.serverProcess = serverProcess;
      this.connectionUri = connectionUri;
      this.workingPath = workingPath;
      this.ports = asList(ports);
    }

    public URI getConnectionUri() {
      return connectionUri;
    }

    @Override
    public void close() {
      try {
        serverProcess.destroyForcibly();
      } finally {
        try {
          serverProcess.waitFor(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        } finally {
          ports.forEach(PortManager.PortRef::close);
        }
      }
    }

    public Path getWorkingArea() {
      return workingPath;
    }
  }
}

