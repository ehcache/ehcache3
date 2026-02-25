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
      checkstyle.setToolVersion("10.18.1");
      checkstyle.setConfigFile(project.getRootProject().file("config/checkstyle.xml"));
      Map<String, Object> properties = checkstyle.getConfigProperties();
      properties.put("projectDir", project.getProjectDir());
      properties.put("rootDir", project.getRootDir());
    });

    project.getConfigurations().named("checkstyle", config -> {
      config.getResolutionStrategy().dependencySubstitution(subs -> {
        subs.substitute(subs.module("org.codehaus.plexus:plexus-utils:3.1.1"))
          .using(subs.module("org.codehaus.plexus:plexus-utils:3.3.0"))
          .because("Checkstyle 10.18.1 pulls mismatched plexus-utils versions");
        subs.substitute(subs.module("org.apache.commons:commons-lang3:3.7"))
          .using(subs.module("org.apache.commons:commons-lang3:3.8.1"))
          .because("Checkstyle transitives mix commons-lang3 versions");
        subs.substitute(subs.module("org.apache.httpcomponents:httpcore:4.4.13"))
          .using(subs.module("org.apache.httpcomponents:httpcore:4.4.14"))
          .because("Align httpcore to latest bugfix release");
        subs.substitute(subs.module("commons-codec:commons-codec:1.11"))
          .using(subs.module("commons-codec:commons-codec:1.15"))
          .because("Checkstyle transitive dependencies depend on different commons-codec versions");
      });
    });
  }
}
