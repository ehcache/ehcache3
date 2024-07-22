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

package org.ehcache.core;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.*;


public class PrefixLoggerTest {

  @Test
  public void testSimplePrefix() {

    Logger mockLogger = mock(Logger.class);
    String prefix = "cache-name";
    PrefixLogger prefixLogger = new PrefixLogger(mockLogger, prefix);
    Object[] parameters = new Object[]{"Message"};
    prefixLogger.info("log-msg{}", parameters);

    verify(mockLogger, times(1)).info(prefix + "log-msg{}", parameters);
  }

  @Test
  public void testEscapedPrefix() {

    Logger mockLogger = mock(Logger.class);
    String prefix = "cache{}name";
    PrefixLogger prefixLogger = new PrefixLogger(mockLogger, prefix);
    Object[] parameters = new Object[]{"Message"};
    prefixLogger.info("log-msg{}", parameters);

    verify(mockLogger, times(1)).info("cache\\{}namelog-msg{}", parameters);
  }


  @Test
  public void testDoubleEscapedPrefix() {

    Logger mockLogger = mock(Logger.class);
    String prefix = "cache\\{}name";
    PrefixLogger prefixLogger = new PrefixLogger(mockLogger, prefix);
    Object[] parameters = new Object[]{"Message"};
    prefixLogger.info("log-msg{}", parameters);

    verify(mockLogger, times(1)).info("cache\\'{''}'namelog-msg{}", parameters);
  }

  @Test
  public void testTripleEscapedPrefix() {

    Logger mockLogger = mock(Logger.class);
    String prefix = "cache\\\\{}name";
    PrefixLogger prefixLogger = new PrefixLogger(mockLogger, prefix);
    Object[] parameters = new Object[]{"Message"};
    prefixLogger.info("log-msg{}", parameters);

    verify(mockLogger, times(1)).info("cache\\\\'{''}'namelog-msg{}", parameters);
  }

  private static final String prefix = "prefix{}";

  @Test
  public void testAllMethodsDelegationWithEnrichment() throws InvocationTargetException, IllegalAccessException {

    Logger mockLogger = mock(Logger.class);
    PrefixLogger prefixLogger = new PrefixLogger(mockLogger, prefix);

    for (Method prefixLogMethod : Logger.class.getDeclaredMethods()) {

      Class<?>[] parameterTypes = prefixLogMethod.getParameterTypes();
      Object[] parameters = new Object[parameterTypes.length];
      for (int j = 0; j < parameterTypes.length; j++) {
        if (String.class.equals(parameterTypes[j])) {
          parameters[j] = "log-msg";
        } else if (parameterTypes[j].isArray()) {
          parameters[j] = new Object[]{};
        } else {
          parameters[j] = mock(parameterTypes[j]);
        }
      }

      prefixLogMethod.invoke(prefixLogger, parameters);
      prefixLogMethod.invoke(verify(mockLogger, times(1)), enrichStringParamValues(parameters, isMsgParameter(prefixLogMethod)));

    }
  }

  private Object[] enrichStringParamValues(Object[] parameters, boolean msgParameter) {

    String enrichPrefix = msgParameter ? prefix : prefix.replace("{}", "\\{}");

    Object[] enrichParams = Arrays.copyOf(parameters, parameters.length);
    for (int i = 0; i < enrichParams.length; i++)
      if (enrichParams[i] instanceof String)
        enrichParams[i] = enrichPrefix + enrichParams[i];

    return enrichParams;
  }

  public boolean isMsgParameter(Method method) {

    Class<?>[] parameterTypes = method.getParameterTypes();

    return parameterTypes.length == 1 && String.class.equals(parameterTypes[0])
      || parameterTypes.length == 2 && String.class.equals(parameterTypes[0]) && Throwable.class.equals(parameterTypes[1])
      || parameterTypes.length == 2 && Marker.class.equals(parameterTypes[0]) && String.class.equals(parameterTypes[1])
      || parameterTypes.length == 3 && Marker.class.equals(parameterTypes[0]) && String.class.equals(parameterTypes[1]) & Throwable.class.equals(parameterTypes[2]);
  }

}
