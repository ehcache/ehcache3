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

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@Parameters(commandNames = "list", commandDescription = "list the cache managers and caches within a cluster")
class ListCacheManagers extends AbstractCommand {

  ListCacheManagers(BaseOptions base) {
    super(base);
  }

  @Override
  public int execute() {
    if (getClusterLocationOverride() == null) {
      throw new ParameterException("--cluster option required with the list command");
    } else {
      System.out.println("Listing cache managers at " + getClusterLocationOverride());
    }
    return 0;
  }
}
