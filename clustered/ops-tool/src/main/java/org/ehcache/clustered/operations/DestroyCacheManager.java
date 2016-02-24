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
package org.ehcache.clustered.operations;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.File;

@Parameters(commandNames = "destroy", commandDescription = "destroy a clustered cache manager, and all of it's caches")
class DestroyCacheManager extends AbstractCommand {

  @Parameter(names = {"-c", "--config"}, required = true, description = "Configuration file to create from")
  private File config;

  @Parameter(names = {"-m", "--match"}, arity = 1, description = "require a matching configuration")
  private boolean requireConfigMatch = true;

  DestroyCacheManager(BaseOptions base) {
    super(base);
  }

  @Override
  public int execute() {
    if (getClusterLocationOverride() == null) {
      System.out.println("Destroying cache manager for config " + config + (isDryRun() ? " [dry-run]" : "") + (requireConfigMatch ? " [matching]" : " [non-matching]"));
    } else {
      System.out.println("Destroying cache manager for config " + config + " at overriding location " + getClusterLocationOverride() + (isDryRun() ? " [dry-run]" : "") + (isDryRun() ? " [matching]" : " [non-matching]"));
    }
    return 0;
  }
}
