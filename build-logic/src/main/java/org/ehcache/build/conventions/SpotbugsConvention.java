package org.ehcache.build.conventions;

import com.github.spotbugs.snom.SpotBugsExtension;
import com.github.spotbugs.snom.SpotBugsPlugin;
import com.github.spotbugs.snom.SpotBugsTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;

public class SpotbugsConvention implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(SpotBugsPlugin.class);

    SpotBugsExtension spotbugs = project.getExtensions().getByType(SpotBugsExtension.class);

    spotbugs.getIgnoreFailures().set(false);
    spotbugs.getToolVersion().set("4.9.8");
    spotbugs.getOmitVisitors().addAll("FindReturnRef", "ConstructorThrow");

    project.getPlugins().withType(JavaBasePlugin.class).configureEach(plugin -> {

      project.getExtensions().configure(JavaPluginExtension.class, java -> {
        java.getSourceSets().configureEach(sourceSet -> {
          project.getDependencies().add(sourceSet.getCompileOnlyConfigurationName(),
            "com.github.spotbugs:spotbugs-annotations:" + spotbugs.getToolVersion().get());
        });

        project.getTasks().withType(SpotBugsTask.class).configureEach(task -> {
          if (task.getName().contains("Test")) {
            task.setEnabled(false);
          } else {
            task.getReports().register("xml", report -> report.setEnabled(true));
            task.getReports().register("html", report -> report.setEnabled(false));
          }
        });
      });

    });


    project.getConfigurations().named("spotbugs", config -> {
      config.getResolutionStrategy().dependencySubstitution(subs -> {
        subs.substitute(subs.module("org.apache.commons:commons-lang3:3.11"))
          .using(subs.module("org.apache.commons:commons-lang3:3.12.0"))
          .because("Spotbugs has dependency divergences");
        subs.substitute(subs.module("org.apache.commons:commons-lang3:3.18.0"))
          .using(subs.module("org.apache.commons:commons-lang3:3.19.0"))
          .because("Spotbugs 4.9.8 has dependency divergences");
        subs.substitute(subs.module("org.apache.logging.log4j:log4j-core:2.25.2"))
          .using(subs.module("org.apache.logging.log4j:log4j-core:2.25.3"))
          .because("Security vulnerability fix");
      });
    });

  }
}
