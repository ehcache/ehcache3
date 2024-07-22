/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.terracotta.testing.config.ConfigRepoStartupBuilder;
import org.terracotta.testing.rules.BasicExternalClusterBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.singletonMap;

public interface StandardCluster {
  static Path clusterPath() {
    return Paths.get("build", "cluster");
  }

  static String offheapResource(String name, long size) {
    return offheapResources(singletonMap(name, size));
  }

  static String offheapResources(Map<String, Long> resources) {
    StringBuilder sb = new StringBuilder("<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>");
    sb.append("<ohr:offheap-resources>");
    for (Map.Entry<String, Long> e : resources.entrySet()) {
      sb.append("<ohr:resource name=\"").append(e.getKey()).append("\" unit=\"MB\">").append(e.getValue()).append("</ohr:resource>");
    }
    sb.append("</ohr:offheap-resources>");
    return sb.append("</config>\n").toString();
  }

  static BasicExternalClusterBuilder newCluster() {
    return BasicExternalClusterBuilder.newCluster().startupBuilder(ConfigRepoStartupBuilder::new);
  }

  static BasicExternalClusterBuilder newCluster(int size) {
    return BasicExternalClusterBuilder.newCluster(size).startupBuilder(ConfigRepoStartupBuilder::new);
  }

  static String leaseLength(Duration leaseLength) {
    return "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
      + "<lease:connection-leasing>"
      + "<lease:lease-length unit='seconds'>" + leaseLength.get(SECONDS) + "</lease:lease-length>"
      + "</lease:connection-leasing>"
      + "</service>";
  }

}
