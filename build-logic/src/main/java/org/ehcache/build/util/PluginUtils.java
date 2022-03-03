package org.ehcache.build.util;

import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Project;
import org.gradle.api.UnknownDomainObjectException;
import org.gradle.api.artifacts.Configuration;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

public class PluginUtils {

  public static NamedDomainObjectProvider<Configuration> registerBucket(Project project, String kind, String name) {
    if (name == null) {
      return registerBucket(project, kind);
    } else {
      NamedDomainObjectProvider<Configuration> bucket = registerBucket(project, lower(camel(name, kind)));
      bucket.configure(c -> c.setDescription(capitalize(kind) + " dependencies for " + name));
      return bucket;
    }
  }

  public static NamedDomainObjectProvider<Configuration> registerBucket(Project project, String kind) {
    try {
      return project.getConfigurations().named(kind);
    } catch (UnknownDomainObjectException ignored) {
      return project.getConfigurations().register(kind, bucket -> {
        bucket.setDescription(capitalize(kind) + " dependencies.");
        bucket.setVisible(false);
        bucket.setCanBeResolved(false);
        bucket.setCanBeConsumed(false);
      });
    }
  }

  private static final Pattern CAMEL_SPLIT = Pattern.compile("(?=\\{Upper})");
  private static final Pattern KEBAB_SPLIT = Pattern.compile("(-)");
  private static final Pattern SPLIT = Pattern.compile(CAMEL_SPLIT.pattern() + "|" + KEBAB_SPLIT.pattern());

  public static String kebab(String ... strings) {
    return stream(strings).filter(Objects::nonNull).flatMap(SPLIT::splitAsStream).collect(joining("-"));
  }

  public static String camel(String ... strings) {
    return stream(strings).filter(Objects::nonNull).flatMap(SPLIT::splitAsStream).map(PluginUtils::capitalize).collect(joining());
  }

  public static String capitalize(String word) {
    return word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1);
  }

  public static String lower(String word) {
    return word.substring(0, 1).toLowerCase(Locale.ROOT) + word.substring(1);
  }
}
