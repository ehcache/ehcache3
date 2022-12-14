package org.ehcache.build.util;

import aQute.bnd.osgi.Builder;
import aQute.bnd.osgi.Jar;
import aQute.service.reporter.Report;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Task;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.ClasspathNormalizer;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

public class OsgiManifestJarExtension {

  private final org.gradle.api.tasks.bundling.Jar jarTask;
  private final MapProperty<String, String> instructions;
  private final ConfigurableFileCollection classpath;
  private final ConfigurableFileCollection sources;

  public OsgiManifestJarExtension(org.gradle.api.tasks.bundling.Jar jarTask) {
    this.jarTask = jarTask;
    this.instructions = jarTask.getProject().getObjects().mapProperty(String.class, String.class);
    this.classpath = jarTask.getProject().getObjects().fileCollection();
    this.sources = jarTask.getProject().getObjects().fileCollection();

    jarTask.getInputs().files(classpath).withNormalizer(ClasspathNormalizer.class).withPropertyName("osgi.classpath");
    jarTask.getInputs().files(sources).withPropertyName("osgi.sources");
    jarTask.getInputs().property("osgi.instructions", (Callable<Map<String, String>>) instructions::get);

    jarTask.getExtensions().add("osgi", this);
    jarTask.doLast("buildManifest", new BuildAction());
  }

  public void instruction(String key, String value) {
    instructions.put(key, value);
  }

  public void instruction(String key, Provider<String> value) {
    instructions.put(key, value);
  }

  @Input @Classpath
  public ConfigurableFileCollection getClasspath() {
    return classpath;
  }

  @InputFiles
  public ConfigurableFileCollection getSources() {
    return sources;
  }

  @Input
  public MapProperty<String, String> getInstructions() {
    return instructions;
  }


  private class BuildAction implements Action<Task> {
    @Override
    public void execute(Task t) {
        try (Builder builder = new Builder()) {
          File archiveFile = jarTask.getArchiveFile().get().getAsFile();

          jarTask.getProject().sync(sync -> sync.from(archiveFile).into(jarTask.getTemporaryDir()));
          File archiveCopyFile = new File(jarTask.getTemporaryDir(), archiveFile.getName());

          Jar bundleJar = new Jar(archiveCopyFile);

          builder.setJar(bundleJar);
          builder.setClasspath(getClasspath().getFiles());
          builder.setSourcepath(getSources().getFiles().toArray(new File[0]));
          builder.addProperties(getInstructions().get());

          try (Jar builtJar = builder.build()) {
            builtJar.write(archiveFile);
          }

          if (!builder.isOk()) {
            jarTask.getProject().delete(archiveFile);
            builder.getErrors().forEach((String msg) -> {
              Report.Location location = builder.getLocation(msg);
              if ((location != null) && (location.file != null)) {
                jarTask.getLogger().error("{}:{}: error: {}", location.file, location.line, msg);
              } else {
                jarTask.getLogger().error("error  : {}", msg);
              }
            });
            throw new GradleException("Bundle " + archiveFile.getName() + " has errors");
          }
        } catch (Exception e) {
          throw new GradleException("Error building bundle", e);
        }
    }
  }
}
