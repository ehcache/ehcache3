/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.spi;

import org.ehcache.spi.service.Service;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.IsSame.sameInstance;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

/**
 *
 * @author cdennis
 */
public class ServiceProviderLifecycleTest {
  
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  @AfterClass
  public static void shutdown() throws InterruptedException {
    EXECUTOR.shutdownNow();
    EXECUTOR.awaitTermination(1, TimeUnit.MINUTES);
  }
  
  @Test
  public void testStartAndStopWait() throws InterruptedException {
    Service service = new StartableService(200, TimeUnit.MILLISECONDS);
    ServiceProvider provider = new ServiceProvider(service);
    long start = System.nanoTime();
    provider.startAllServices();
    long end = System.nanoTime();
    assertThat(end - start, greaterThan(TimeUnit.MILLISECONDS.toNanos(100)));
    assertThat(provider.findService(DelayedService.class), sameInstance(service));
    
    start = System.nanoTime();
    provider.stopAllServices();
    end = System.nanoTime();
    assertThat(end - start, greaterThan(TimeUnit.MILLISECONDS.toNanos(100)));
    assertThat(provider.findService(DelayedService.class), sameInstance(service));
  }

  @Test
  public void testRecycleStartAndStop() throws InterruptedException {
    Service service = new StartableService(200, TimeUnit.MILLISECONDS);
    ServiceProvider provider = new ServiceProvider(service);
    provider.startAllServices();
    assertThat(provider.findService(DelayedService.class), sameInstance(service));
    provider.stopAllServices();
    
    long start = System.nanoTime();
    provider.startAllServices();
    long end = System.nanoTime();
    assertThat(end - start, greaterThan(TimeUnit.MILLISECONDS.toNanos(100)));
    assertThat(provider.findService(DelayedService.class), sameInstance(service));
    
    start = System.nanoTime();
    provider.stopAllServices();
    end = System.nanoTime();
    assertThat(end - start, greaterThan(TimeUnit.MILLISECONDS.toNanos(100)));
    assertThat(provider.findService(DelayedService.class), sameInstance(service));
  }
  interface DelayedService extends Service {
    
  }

  static class StartableService implements DelayedService {

    private final TimeUnit unit;
    private final long time;
    
    public StartableService(long time, TimeUnit unit) {
      this.time = time;
      this.unit = unit;
    }
    @Override
    public Future<?> start() {
      return EXECUTOR.submit(new DummyTask(time, unit));
    }

    @Override
    public Future<?> stop() {
      return EXECUTOR.submit(new DummyTask(time, unit));
    }
  }

  static class DummyTask implements Callable<Object> {

    private final long time;
    private final TimeUnit unit;

    public DummyTask(long time, TimeUnit unit) {
      this.time = time;
      this.unit = unit;
    }
    
    
    @Override
    public Object call() throws InterruptedException {
      unit.sleep(time);
      return null;
    }
    
  }
}
