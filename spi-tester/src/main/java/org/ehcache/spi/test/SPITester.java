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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * @author Hung Huynh
 */
public abstract class SPITester {

  public Result runTestSuite() {
    Result result = new Result();
    result.testRunStarted();
    Class<? extends SPITester> testClass = getClass();
    ArrayList<Method> beforeMethodList = new ArrayList<>();
    ArrayList<Method> afterMethodList = new ArrayList<>();
    for(Method m : testClass.getDeclaredMethods()){
      if (m.isAnnotationPresent(Before.class)) {
        beforeMethodList.add(m);
      }
      if (m.isAnnotationPresent(After.class)) {
        afterMethodList.add(m);
      }
    }

    for (Method m : testClass.getDeclaredMethods()) {
      if (m.isAnnotationPresent(SPITest.class)) {
        if (m.isAnnotationPresent(Ignore.class)) {
          result.testSkipped(new ResultState(testClass, m.getName(), m.getAnnotation(Ignore.class).reason()));
        }
        else try {
          for (Method bm : beforeMethodList) {
            try {
              bm.invoke(this, (Object[]) null);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          m.invoke(this, (Object[]) null);
          result.testFinished();
        } catch (InvocationTargetException wrappedExc) {
          if(wrappedExc.getTargetException() instanceof LegalSPITesterException) {
            result.testsOverlookedDueToLegalException(new ResultState(testClass, m.getName(), wrappedExc.getCause()));
          }
          else {
            result.testFailed(new ResultState(testClass, m.getName(), wrappedExc.getCause()));
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (!afterMethodList.isEmpty()) {
            for (Method am : afterMethodList) {
              try {
                am.invoke(this, (Object[]) null);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }
        }
      }
    }
    result.testRunFinished();
    return result;
  }
}
