package org.ehcache.build.util;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;

import java.util.Locale;

public class PluginUtils {

  public static Configuration createBucket(Project project, String kind, String variant) {
    if (variant == null) {
      return createBucket(project, kind);
    } else {
      Configuration configuration = project.getConfigurations().maybeCreate(variant + capitalize(kind));
      configuration.setDescription(capitalize(kind) + " dependencies for " + variant);
      configuration.setVisible(false);
      configuration.setCanBeResolved(false);
      configuration.setCanBeConsumed(false);
      return configuration;
    }
  }

  public static Configuration createBucket(Project project, String kind) {
    Configuration configuration = project.getConfigurations().maybeCreate(kind);
    configuration.setDescription(capitalize(kind) + " dependencies");
    configuration.setVisible(false);
    configuration.setCanBeResolved(false);
    configuration.setCanBeConsumed(false);
    return configuration;
  }

  public static Configuration bucket(Project project, String kind, String variant) {
    if (variant == null) {
      return bucket(project, kind);
    } else {
      return project.getConfigurations().getByName(variant + capitalize(kind));
    }
  }

  public static Configuration bucket(Project project, String kind) {
    return project.getConfigurations().getByName(kind);
  }

  public static String capitalize(String word) {
    return word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1);
  }

}
