package org.ehcache.build.plugins;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.internal.lambdas.SerializableLambdas;
import org.gradle.api.internal.tasks.DefaultSourceSet;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.JavaCompile;

import static org.gradle.api.plugins.internal.JvmPluginsHelper.configureOutputDirectoryForSourceSet;

public class UnsafeJavaPlugin implements Plugin<Project> {

  public void apply(Project project) {
    project.getPluginManager().apply(JavaBasePlugin.class);
    project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().all(sourceSet -> {
      SourceDirectorySet unsafeSource = project.getObjects().sourceDirectorySet("unsafe", ((DefaultSourceSet) sourceSet).getDisplayName() + " unsafe Java source");
      sourceSet.getExtensions().add("unsafe", unsafeSource);
      unsafeSource.srcDir("src/" + sourceSet.getName() + "/unsafe");
      sourceSet.getResources().getFilter().exclude(SerializableLambdas.spec(element -> unsafeSource.contains(element.getFile())));
      sourceSet.getAllJava().source(unsafeSource);
      sourceSet.getAllSource().source(unsafeSource);

      TaskProvider<JavaCompile> unsafeCompile = project.getTasks().register(sourceSet.getCompileTaskName("unsafe"), JavaCompile.class, compile -> {
        compile.setDescription("Compiles the " + unsafeSource.getDisplayName() + ".");
        compile.setSource(unsafeSource);
        compile.setClasspath(sourceSet.getCompileClasspath());
      });

      project.getTasks().named(sourceSet.getCompileJavaTaskName(), JavaCompile.class, compile -> {
        compile.setClasspath(compile.getClasspath().plus(project.files(unsafeSource.getClassesDirectory())));
      });

      configureOutputDirectoryForSourceSet(sourceSet, unsafeSource, project, unsafeCompile, unsafeCompile.map(JavaCompile::getOptions));
      project.getTasks().named(sourceSet.getClassesTaskName(), (task) -> task.dependsOn(unsafeCompile));
    });
  }
}
