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

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.String.join;
import static java.nio.file.Files.find;
import static java.nio.file.Files.isRegularFile;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.cleanCaches;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.systemPackages;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.CoreOptions.workingDirectory;
import static org.ops4j.pax.exam.CoreOptions.wrappedBundle;

public class OsgiTestUtils {

  public static Option baseConfiguration(String ... path) {
    return composite(
      gradleBundle("org.slf4j:slf4j-api"),
      gradleBundle("org.slf4j:slf4j-simple").noStart(),
      gradleBundle("org.apache.felix:org.apache.felix.scr"),
      systemProperty("pax.exam.osgi.unresolved.fail").value("true"),
      systemPackages(
        "javax.xml.bind;version=2.3.0",
        "javax.xml.bind.annotation;version=2.3.0",
        "javax.xml.bind.annotation.adapters;version=2.3.0"
      ),
      cleanCaches(true),
      workingDirectory(join(File.separator, "build", "osgi-container", join(File.separator, path))),
      junitBundles()
    );
  }

  public static UrlProvisionOption gradleBundle(String module) {
    return bundle(artifact(module).toUri().toString());
  }

  public static WrappedUrlProvisionOption wrappedGradleBundle(String module) {
    return wrappedBundle(artifact(module).toUri().toString());
  }

  private static Path artifact(String module) {
    Path path = Paths.get(requireNonNull(System.getProperty(module + ":osgi-path"), module + " not available"));
    if (isRegularFile(path)) {
      return path;
    } else {
      throw new IllegalArgumentException("Module '" + module + "' not found at " + path);
    }
  }

  public static Cluster startServer(Path serverDirectory) throws IOException {
    Path kitLocation = Paths.get(System.getProperty("kitInstallationPath"));

    Path configFile = serverDirectory.resolve("tc-config.xml");
    int tsaPort = selectAvailableEphemeralPort();
    int tsaGroupPort = selectAvailableEphemeralPort();

    try (PrintWriter writer = new PrintWriter(new FileWriter(configFile.toFile()))) {
      writer.println("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
      writer.println("<tc-config xmlns=\"http://www.terracotta.org/config\">");
      writer.println("<plugins>");
      writer.println("<config>");
      writer.println("<ohr:offheap-resources xmlns:ohr=\"http://www.terracotta.org/config/offheap-resource\">");
      writer.println("<ohr:resource name=\"main\" unit=\"MB\">32</ohr:resource>");
      writer.println("</ohr:offheap-resources>");
      writer.println("</config>");
      writer.println("</plugins>");
      writer.println("<servers>");
      writer.println("<server host=\"localhost\" name=\"default-server\" bind=\"0.0.0.0\">");
      writer.println("<logs>" + serverDirectory.toString() + "</logs>");
      writer.println("<tsa-port bind=\"0.0.0.0\">" + tsaPort + "</tsa-port>");
      writer.println("<tsa-group-port bind=\"0.0.0.0\">" + tsaGroupPort + "</tsa-group-port>");
      writer.println("</server>");
      writer.println("<client-reconnect-window>120</client-reconnect-window>");
      writer.println("</servers>");
      writer.println("<failover-priority><availability/></failover-priority>");
      writer.println("</tc-config>");
    }


    Path serverDir = kitLocation.resolve("server");

    String pluginClasspath = Stream.of(
      serverDir.resolve("plugins").resolve("lib"),
      serverDir.resolve("plugins").resolve("api")
    ).flatMap(dir -> {
      try {
        return find(dir, 10, (p, a) -> a.isRegularFile() && p.getFileName().toString().endsWith(".jar"));
      } catch (IOException e) {
        return Stream.empty();
      }
    }).map(p -> p.toString()).collect(joining(File.pathSeparator));

    ProcessBuilder serverProcess = new ProcessBuilder()
      .directory(serverDirectory.toFile())
      .command(Paths.get(System.getProperty("java.home")).resolve("bin")
          .resolve(System.getProperty("os.name").contains("Windows") ? "java.exe" : "java").toString());

    String tcServerOptions = System.getProperty("tc-server-opts");
    if (tcServerOptions != null) {
      serverProcess.command().addAll(asList(tcServerOptions.split("\\s")));
    }
    serverProcess.command().addAll(asList(
      "-Dtc.install-root=" + serverDir,
      "-cp", serverDir.resolve("lib").resolve("tc.jar") + File.pathSeparator + pluginClasspath,
      "com.tc.server.TCServerMain",
      "-f", configFile.toString()));
    serverProcess.inheritIO();

    return new Cluster(serverProcess.start(), URI.create("terracotta://localhost:" + tsaPort), serverDirectory);
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

    Cluster(Process serverProcess, URI connectionUri, Path workingPath) {
      this.serverProcess = serverProcess;
      this.connectionUri = connectionUri;
      this.workingPath = workingPath;
    }

    public URI getConnectionUri() {
      return connectionUri;
    }

    @Override
    public void close() throws IOException {
      try {
        serverProcess.destroyForcibly();
      } finally {
        try {
          serverProcess.waitFor(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
      }
    }

    public Path getWorkingArea() {
      return workingPath;
    }
  }
}

