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
    // Later versions of Spotbugs have stupid heuristics for EI_EXPOSE_REP*
    spotbugs.getToolVersion().set("4.2.3");

    project.getPlugins().withType(JavaBasePlugin.class, plugin -> {

      project.getExtensions().configure(JavaPluginExtension.class, java -> {
        java.getSourceSets().configureEach(sourceSet -> {
          project.getDependencies().add(sourceSet.getCompileOnlyConfigurationName(),
            "com.github.spotbugs:spotbugs-annotations:" + spotbugs.getToolVersion().get());
        });

        project.getTasks().withType(SpotBugsTask.class, task -> {
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
      });
    });

  }
}
