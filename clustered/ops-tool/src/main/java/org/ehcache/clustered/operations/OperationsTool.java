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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.util.Comparator;

public class OperationsTool {

  private static final Comparator<? super ParameterDescription> REQUIRED_FIRST = (Comparator<ParameterDescription>) (a, b) -> {
    if (a.getParameter().required() == b.getParameter().required()) {
      return a.getLongestName().compareTo(b.getLongestName());
    } else if (a.getParameter().required()) {
      return -1;
    } else {
      return 1;
    }
  };

  public static void main(String[] args) {
    System.exit(innerMain(args));
  }

  public static int innerMain(String[] args) {
    BaseOptions base = new BaseOptions();
    JCommander jc = new JCommander(base);
    jc.setProgramName("ehcache-ops");
    jc.addCommand(new ListCacheManagers(base));
    jc.addCommand(new CreateCacheManager(base));
    jc.addCommand(new UpdateCacheManager(base));
    jc.addCommand(new DestroyCacheManager(base));

    jc.setParameterDescriptionComparator(REQUIRED_FIRST);
    for (JCommander jcc : jc.getCommands().values()) {
      jcc.setParameterDescriptionComparator(REQUIRED_FIRST);
    }

    try {
      jc.parse(args);

      if (base.isHelp()) {
        return usage(jc, new StringBuilder());
      } else {
        int result = 0;
        for (Object o : jc.getCommands().get(jc.getParsedCommand()).getObjects()) {
          result |= ((Command) o).execute();
        }

        return result;
      }
    } catch (ParameterException e) {
      return usage(jc, new StringBuilder(e.getMessage()).append("\n"));
    }
  }

  private static int usage(JCommander jc, StringBuilder prefix) {
    jc.usage(prefix);
    System.err.println(prefix.toString());
    return 1;
  }
}
