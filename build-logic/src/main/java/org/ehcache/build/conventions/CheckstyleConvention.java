package org.ehcache.build.conventions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.quality.CheckstyleExtension;
import org.gradle.api.plugins.quality.CheckstylePlugin;

import java.util.Map;

public class CheckstyleConvention implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getPlugins().apply(CheckstylePlugin.class);

    project.getExtensions().configure(CheckstyleExtension.class, checkstyle -> {
      checkstyle.setConfigFile(project.getRootProject().file("config/checkstyle.xml"));
      Map<String, Object> properties = checkstyle.getConfigProperties();
      properties.put("projectDir", project.getProjectDir());
      properties.put("rootDir", project.getRootDir());
    });
  }
}
