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
package org.ehcache.integration.util;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static java.io.File.separator;
import static java.util.Arrays.asList;

public final class JavaExec {

  private JavaExec() {
  }

  public static CompletableFuture<Integer> exec(Class<?> klass, String ... args) throws IOException, InterruptedException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + separator + "bin" + separator + "java";
    String classpath = System.getProperty("java.class.path");
    String className = klass.getName();

    ProcessBuilder builder = new ProcessBuilder(
      javaBin, "-cp", classpath, className);
    builder.command().addAll(asList(args));

    Process process = builder.inheritIO().start();

    return CompletableFuture.supplyAsync(() -> {
      while (process.isAlive()) {
        try {
          process.waitFor();
        } catch (InterruptedException e) {
          // This should not happen but continue spinning if it does since the actual process is not done yet
        }
      }
      return process.exitValue();
    });
  }

}
