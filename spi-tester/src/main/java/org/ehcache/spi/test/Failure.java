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

package org.ehcache.spi.test;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Hung Huynh
 */
public class Failure {
  private final String    testMethod;
  private final Throwable thrownException;

  public Failure(String testMethod, Throwable thrownException) {
    this.testMethod = testMethod;
    this.thrownException = thrownException;
  }

  public String getTestMethod() {
    return testMethod;
  }

  public Throwable getThrownException() {
    return thrownException;
  }

  public String getTrace() {
    StringWriter stringWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(stringWriter);
    thrownException.printStackTrace(writer);
    StringBuffer buffer = stringWriter.getBuffer();
    return buffer.toString();
  }

}
