#!/bin/bash
//usr/bin/env groovy -cp client/ehcache/ehcache-@version@.jar:client/ehcache/ehcache-clustered-@version@.jar "$0" $@; exit $?

import org.ehcache.Cache
import org.ehcache.PersistentCacheManager
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder
import org.ehcache.config.builders.CacheConfigurationBuilder
import org.ehcache.config.builders.CacheManagerBuilder
import org.ehcache.config.builders.ResourcePoolsBuilder
import org.ehcache.config.units.MemoryUnit

Closure<?> ehcacheClientTest = { int port ->
  println "Connecting Ehcache client..."
  CacheManagerBuilder.newCacheManagerBuilder()
    .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost:${port}/my-cache-manager-name"))
      .autoCreateOnReconnect(server -> server.defaultServerResource("offheap-1")))
    .build(true)
    .withCloseable { PersistentCacheManager cacheManager ->
      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(
        Long.class,
        String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("offheap-1", 2, MemoryUnit.MB)))
        .build())
      println "put()..."
      cache.put(42L, "All you need to know!")
      println "get()..."
      assert cache.get(42L) == "All you need to know!"
    }
}

Closure<?> run = { String cmd, Closure<?> result = null ->
  println "> ${cmd}"
  cmd.execute().with { process ->
    if (result) {
      int status = process.waitFor()
      if (status != 0) throw new IllegalStateException("Command failed:\n${cmd}\nError:\n${process.err.text}")
      process.text.trim().with { stdout ->
        println stdout
        result(stdout)
      }
    } else {
      process.consumeProcessOutput(System.out, System.err)
      int status = process.waitFor()
      process.waitForProcessOutput()
      if (status != 0) throw new IllegalStateException("Command failed: ${status}")
    }
  }
}

Deque<Closure<?>> cleanups = new ArrayDeque<>()

Runtime.runtime.addShutdownHook {
  println """>
> CLEANUP...
>"""
  while (cleanups) {
    cleanups.pollLast().call()
  }
}

println """>
> BUILDING...
>"""

run "docker build -f docker/server/Dockerfile -t ehcache-terracotta-server:@version@ ."
run "docker build -f docker/config-tool/Dockerfile -t ehcache-terracotta-config-tool:@version@ ."
run "docker build -f docker/voter/Dockerfile -t ehcache-terracotta-voter:@version@ ."

println """>
> TESTING...
>"""

run "docker network create net-${UUID.randomUUID()}", { network ->
  cleanups << {
    run "docker network rm ${network}"
  }

  run "docker run --detach --rm -p 9410:9410 -e DEFAULT_ACTIVATE=true -e DEFAULT_FAILOVER=consistency:1 -h ehcache-terracotta-server --network ${network} --user 1234:0 ehcache-terracotta-server:@version@", { server ->
    cleanups << {
      run "docker rm -f ${server}"
    }

    Thread.startDaemon {
      run "docker logs --follow ${server}"
    }

    sleep(10_000)

    run "docker run --detach --rm  --network ${network} --user 1234:0 ehcache-terracotta-voter:@version@ -connect-to ehcache-terracotta-server:9410", { voter ->
      cleanups << {
        run "docker rm -f ${voter}"
      }

      Thread.startDaemon {
        run "docker logs --follow ${voter}"
      }

      sleep(10_000)

      run "docker run --rm --network ${network} --user 1234:0 ehcache-terracotta-config-tool:@version@ diagnostic -connect-to ehcache-terracotta-server"

      ehcacheClientTest(9410)
    }
  }
}
