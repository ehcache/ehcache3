package org.ehcache.build.plugins.osgids;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

public class OsgiDsPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getExtensions().configure(SourceSetContainer.class, sourceSets -> sourceSets.configureEach(sourceSet -> {
      String taskName = sourceSet.getTaskName("generate", "DeclarativeServicesDescriptors");
      TaskProvider<GenerateDeclarativeServicesDescriptors> generateTask = project.getTasks().register(taskName, GenerateDeclarativeServicesDescriptors.class, task -> {
        task.setDescription("Generate OSGi Declarative Services XML descriptors for " + sourceSet.getName() + " classes");
        task.getInputFiles().from(sourceSet.getOutput().getClassesDirs());
        task.getClasspath().from(sourceSet.getRuntimeClasspath());
        task.getOutputDirectory().set(project.getLayout().getBuildDirectory().dir("generated/resources/osgi-ds/" + sourceSet.getName()));
      });
      sourceSet.getOutput().getGeneratedSourcesDirs().plus(project.fileTree(generateTask));
    }));
  }
}
