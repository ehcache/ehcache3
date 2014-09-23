package org.ehcache.spi.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.ehcache.spi.cache.Store;

public abstract class SPITester<T> {

  protected final Factory<T> factory;

  public SPITester(final Factory<T> factory) {
    this.factory = factory;
  }

  public Result runTestSuite() {
    Result result = new Result();
    result.testRunStarted();
    for (Method m : getClass().getDeclaredMethods()) {
      if (m.isAnnotationPresent(SPITest.class)) {
        try {
          //Class<?> testClass = m.getClass();
          // Constructor<?> ctor =
          // testClass.getConstructor(ServiceFactory.class);
          // Object object = ctor.newInstance(new Object[] { factory });
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
