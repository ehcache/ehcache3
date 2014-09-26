package org.ehcache.spi.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Hung Huynh
 */
public abstract class SPITester {

  public Result runTestSuite() {
    Result result = new Result();
    result.testRunStarted();
    for (Method m : getClass().getDeclaredMethods()) {
      if (m.isAnnotationPresent(SPITest.class)) {
        try {
          m.invoke(this, (Object[]) null);
          result.testFinished();
        } catch (InvocationTargetException wrappedExc) {
          Failure failure = new Failure(m.getName(), wrappedExc.getCause());
          result.testFailure(failure);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    result.testRunFinished();
    return result;
  }
}
