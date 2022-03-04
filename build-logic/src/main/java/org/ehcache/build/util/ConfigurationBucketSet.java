package org.ehcache.build.util;

import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.SourceSet;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.ehcache.build.util.PluginUtils.registerBucket;
import static org.gradle.api.plugins.JavaPlugin.API_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.COMPILE_ONLY_API_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME;

public class ConfigurationBucketSet {
  private final Map<String, NamedDomainObjectProvider<Configuration>> buckets;

  private ConfigurationBucketSet(Map<String, NamedDomainObjectProvider<Configuration>> buckets) {
    this.buckets = unmodifiableMap(buckets);
  }

  public NamedDomainObjectProvider<Configuration> api() {
    return bucket(API_CONFIGURATION_NAME);
  }

  public NamedDomainObjectProvider<Configuration> implementation() {
    return bucket(IMPLEMENTATION_CONFIGURATION_NAME);
  }

  public NamedDomainObjectProvider<Configuration> compileOnly() {
    return bucket(COMPILE_ONLY_CONFIGURATION_NAME);
  }

  public NamedDomainObjectProvider<Configuration> compileOnlyApi() {
    return bucket(COMPILE_ONLY_API_CONFIGURATION_NAME);
  }

  public NamedDomainObjectProvider<Configuration> runtimeOnly() {
    return bucket(RUNTIME_ONLY_CONFIGURATION_NAME);
  }

  public NamedDomainObjectProvider<Configuration> bucket(String kind) {
    return buckets.get(kind);
  }


  public ConfigurationBucketSet extendFrom(ConfigurationBucketSet dependency) {
    buckets.forEach((kind, bucket) -> {
      bucket.configure(c -> {
        NamedDomainObjectProvider<Configuration> b = dependency.bucket(kind);
        if (b != null) {
          c.extendsFrom(b.get());
        }
      });
    });
    return this;
  }

  public static ConfigurationBucketSet bucketsFor(Project project, String name, String... additionalKinds) {
    ConfigurationBucketSet bucket = new ConfigurationBucketSet(
      concat(of(
          API_CONFIGURATION_NAME,
          IMPLEMENTATION_CONFIGURATION_NAME,
          COMPILE_ONLY_CONFIGURATION_NAME,
          COMPILE_ONLY_API_CONFIGURATION_NAME,
          RUNTIME_ONLY_CONFIGURATION_NAME),
        of(additionalKinds)
      ).collect(Collectors.toMap(Function.identity(), kind -> registerBucket(project, kind, name)))
    );
    bucket.implementation().configure(c -> c.extendsFrom(bucket.api().get()));
    bucket.compileOnly().configure(c -> c.extendsFrom(bucket.compileOnlyApi().get()));
    return bucket;
  }

  public static ConfigurationBucketSet bucketsFor(Project project, SourceSet sourceSet) {
    Map<String, NamedDomainObjectProvider<Configuration>> buckets = new HashMap<>();
    buckets.put(API_CONFIGURATION_NAME, project.getConfigurations().named(sourceSet.getApiConfigurationName()));
    buckets.put(IMPLEMENTATION_CONFIGURATION_NAME, project.getConfigurations().named(sourceSet.getImplementationConfigurationName()));
    buckets.put(COMPILE_ONLY_CONFIGURATION_NAME, project.getConfigurations().named(sourceSet.getCompileOnlyConfigurationName()));
    buckets.put(COMPILE_ONLY_API_CONFIGURATION_NAME, project.getConfigurations().named(sourceSet.getCompileOnlyConfigurationName()));
    buckets.put(RUNTIME_ONLY_CONFIGURATION_NAME, project.getConfigurations().named(sourceSet.getRuntimeOnlyConfigurationName()));
    return new ConfigurationBucketSet(buckets);
  }
}
