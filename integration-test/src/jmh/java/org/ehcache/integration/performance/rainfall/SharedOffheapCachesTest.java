package org.ehcache.integration.performance.rainfall;

import io.rainfall.ObjectGenerator;
import io.rainfall.Runner;
import io.rainfall.SyntaxException;
import io.rainfall.configuration.ConcurrencyConfig;
import io.rainfall.ehcache.statistics.EhcacheResult;
import io.rainfall.generator.LongGenerator;
import io.rainfall.generator.StringGenerator;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.rainfall.Scenario.scenario;
import static io.rainfall.configuration.ReportingConfig.report;
import static io.rainfall.configuration.ReportingConfig.text;
import static io.rainfall.ehcache.statistics.EhcacheResult.PUT;
import static io.rainfall.ehcache3.CacheDefinition.cache;
import static io.rainfall.ehcache3.Ehcache3Operations.put;
import static io.rainfall.execution.Executions.during;
import static io.rainfall.execution.Executions.times;
import static io.rainfall.generator.SequencesGenerator.sequentially;
import static io.rainfall.unit.TimeDivision.seconds;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * @author Aurelien Broszniowski
 */

public class SharedOffheapCachesTest {

  @Test
  public void testPutSharedOffHeap() throws SyntaxException {
    CacheConfigurationBuilder<Long, String> cacheBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      newResourcePoolsBuilder()
        .heap(10_000, EntryUnit.ENTRIES)
        .sharedOffheap()
        .build());

    CacheManager cacheManager = newCacheManagerBuilder()
      .sharedResources(ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(64, MemoryUnit.MB))
      .withCache("one", cacheBuilder.build())
      .withCache("two", cacheBuilder.build())
      .build(true);

    testPut(cacheManager);
  }

  @Test
  public void testPutNotSharedOffHeap() throws SyntaxException {
    CacheConfigurationBuilder<Long, String> cacheBuilder = newCacheConfigurationBuilder(Long.class, String.class,
      newResourcePoolsBuilder()
        .heap(10_000, EntryUnit.ENTRIES)
        .offheap(32, MemoryUnit.MB)
        .build());

    CacheManager cacheManager = newCacheManagerBuilder()
      .withCache("one", cacheBuilder.build())
      .withCache("two", cacheBuilder.build())
      .build(true);

    testPut(cacheManager);
  }


  public void testPut(CacheManager cacheManager) throws SyntaxException {
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(4).timeout(30, MINUTES);

    ObjectGenerator<Long> keyGenerator = new LongGenerator();
    ObjectGenerator<String> valueGenerator = new StringGenerator(100);

    Cache<Long, String> one = cacheManager.getCache("one", Long.class, String.class);
    Cache<Long, String> two = cacheManager.getCache("two", Long.class, String.class);
    try {
      long start = System.nanoTime();

      Runner.setUp(
          scenario("Cache Put")
            .exec(put(keyGenerator, valueGenerator, sequentially(),
              Arrays.asList(cache("one", one), cache("two", two)))))
        .warmup(during(90, seconds))
        .executed(times(100_000_000))
        .config(concurrency)
        .config(report(EhcacheResult.class, new EhcacheResult[]{PUT}).log(text())
        )
        .start()
      ;

      long end = System.nanoTime();
      System.out.println("Warmup time = " + TimeUnit.NANOSECONDS.toMillis((end - start)) + "ms");
    } finally {
      cacheManager.close();
    }
  }

}
