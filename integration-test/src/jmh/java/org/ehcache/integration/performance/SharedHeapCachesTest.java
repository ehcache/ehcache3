package org.ehcache.integration.performance;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author Aurelien Broszniowski
 */

@State(Scope.Benchmark)
@Threads(16)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SharedHeapCachesTest {

  public static void main(String[] args) throws RunnerException {
    Options options = new OptionsBuilder()
      .include(SharedHeapCachesTest.class.getSimpleName())
      .forks(0)
      .build();

    new Runner(options).run();
  }

  Cache<Long, String> cache1;
  Cache<Long, String> cache2;
  private static final long ENTRIES_COUNT = 100_000;

  @Setup(Level.Iteration)
  public void setup() throws Exception {
    CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().sharedResources(
      ResourcePoolsBuilder.newResourcePoolsBuilder().heap(200_000, EntryUnit.ENTRIES)).build(true);
    cache1 = manager.createCache("cache1",
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().sharedHeap()));
    cache2 = manager.createCache("cache2",
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder().sharedHeap()));

    for (long i = 0; i < ENTRIES_COUNT; i++) {
      cache1.put(i, String.valueOf(i));
      cache2.put(i, String.valueOf(i));
    }
  }

  @Benchmark
  public void testRemove() throws Exception {
    for (long i = 0; i < ENTRIES_COUNT; i++) {
      cache1.remove(i);
      cache2.remove(i);
    }
  }

  @Benchmark
  public void testPut() throws Exception {
    for (long i = 0; i < ENTRIES_COUNT; i++) {
      cache1.put(i, String.valueOf(i));
      cache2.put(i, String.valueOf(i));
    }
  }

  @Benchmark
  public void testUpdate() throws Exception {
    for (long i = 0; i < ENTRIES_COUNT; i++) {
      cache1.put(i, String.valueOf(i + 1));
      cache2.put(i, String.valueOf(i + 1));
    }
  }

  @Benchmark
  public void testGet(Blackhole bh) throws Exception {
    for (long i = 0; i < ENTRIES_COUNT; i++) {
      String s1 = cache1.get(i);
      String s2 = cache2.get(i);
      bh.consume(s1);
      bh.consume(s2);
    }
  }
}
