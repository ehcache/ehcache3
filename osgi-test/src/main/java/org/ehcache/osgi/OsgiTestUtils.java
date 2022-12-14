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
import org.ops4j.pax.exam.options.ProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;
import org.osgi.framework.Constants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.lang.String.join;
import static java.lang.System.getProperty;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.cleanCaches;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.CoreOptions.workingDirectory;
import static org.ops4j.pax.exam.CoreOptions.wrappedBundle;
import static org.ops4j.pax.exam.options.WrappedUrlProvisionOption.OverwriteMode.MERGE;

public class OsgiTestUtils {

  public static Option baseConfiguration(String ... path) {
    return composite(
      gradleBundle("org.slf4j:slf4j-api"),
      gradleBundle("org.slf4j:slf4j-simple").noStart(),
      gradleBundle("org.apache.felix:org.apache.felix.scr"),
      systemProperty("pax.exam.osgi.unresolved.fail").value("true"),
      cleanCaches(true),
      workingDirectory(join(File.separator, "build", "osgi-container", join(File.separator, path))),
      junitBundles()
    );
  }

  public static Option jaxbConfiguration() {
    return optionalGradleBundle("com.sun.xml.bind:jaxb-osgi")
      .map(jaxb -> composite(jaxb,
        gradleBundle("javax.xml.bind:jaxb-api"),
        gradleBundle("com.sun.activation:javax.activation"),
        gradleBundle("org.glassfish.hk2:osgi-resource-locator"))
      ).orElseGet(() -> optionalGradleBundle("org.glassfish.jaxb:jaxb-runtime")
        .map(jaxb -> composite(jaxb,
          wrappedGradleBundle("javax.xml.bind:jaxb-api").instructions("-removeheaders=Require-Capability"),
          gradleBundle("com.sun.istack:istack-commons-runtime"),
          gradleBundle("com.sun.activation:javax.activation"),
          gradleBundle("org.glassfish.hk2:osgi-resource-locator"))
        ).orElseThrow(AssertionError::new));
  }

  public static Option jtaConfiguration() {
    return composite(
      wrappedGradleBundle("javax.transaction:jta").instructions("Fragment-Host=org.apache.felix.framework"),
      gradleBundle("org.codehaus.btm:btm")
    );
  }

  public static ProvisionOption<?> gradleBundle(String module) {
    return optionalGradleBundle(module).orElseThrow(() -> new IllegalArgumentException("Cannot find '" + module + "'"));
  }

  private static final Attributes.Name BUNDLE_SYMBOLICNAME = new Attributes.Name(Constants.BUNDLE_SYMBOLICNAME);

  public static Optional<ProvisionOption<?>> optionalGradleBundle(String module) {
    return artifact(module).map(artifact -> {
      try (JarFile jar = new JarFile(artifact.toFile())) {
        Manifest manifest = jar.getManifest();
        if (manifest != null && manifest.getMainAttributes().containsKey(BUNDLE_SYMBOLICNAME)) {
          return bundle(artifact.toUri().toString());
        } else {
          return wrappedBundle(artifact.toUri().toString());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Module '" + module + "' artifact " + artifact + " is broken?");
      }
    });
  }

  public static WrappedUrlProvisionOption wrappedGradleBundle(String module) {
    ProvisionOption<?> provisionOption = gradleBundle(module);
    if (provisionOption instanceof WrappedUrlProvisionOption) {
      return (WrappedUrlProvisionOption) provisionOption;
    } else {
      return wrappedBundle(provisionOption.getURL()).overwriteManifest(MERGE);
    }
  }

  private static Optional<Path> artifact(String module) {
    return Optional.ofNullable(getProperty(module + ":osgi-path")).map(Paths::get).filter(Files::isRegularFile);
  }
}

