package org.ehcache.build.plugins.osgids;

import org.apache.felix.scrplugin.Options;
import org.apache.felix.scrplugin.Project;
import org.apache.felix.scrplugin.SCRDescriptorException;
import org.apache.felix.scrplugin.SCRDescriptorFailureException;
import org.apache.felix.scrplugin.SCRDescriptorGenerator;
import org.apache.felix.scrplugin.Source;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.EmptyFileVisitor;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileVisitDetails;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public abstract class GenerateDeclarativeServicesDescriptors extends DefaultTask {

  @InputFiles
  public abstract ConfigurableFileCollection getInputFiles();

  @Classpath
  public abstract ConfigurableFileCollection getClasspath();

  @OutputDirectory
  public abstract DirectoryProperty getOutputDirectory();

  @TaskAction
  public void generateDeclarativeServicesDescriptors() throws SCRDescriptorException, SCRDescriptorFailureException, IOException {
    final Options scrOptions = createOptions();

    try (GradleScrProject scrProject = new GradleScrProject(getInputFiles(), getClasspath())) {
      final SCRDescriptorGenerator scrGenerator = new SCRDescriptorGenerator(new ScrLoggerAdapter(getLogger()));
      scrGenerator.setOptions(scrOptions);
      scrGenerator.setProject(scrProject);

      scrGenerator.execute();
    }
  }

  private Options createOptions() {
    final Options scrOptions = new Options();
    scrOptions.setOutputDirectory(getOutputDirectory().get().getAsFile());
    scrOptions.setStrictMode(false);
    scrOptions.setSpecVersion(null);

    return scrOptions;
  }

  static class GradleScrProject extends Project implements Closeable {

    private final URLClassLoader urlClassLoader;

    GradleScrProject(FileCollection input, FileCollection classpath) {
      Set<File> classpathFiles = classpath.getFiles();
      URL[] classpathUrls = classpathFiles.stream().map(f -> {
        try {
          return f.toURI().toURL();
        } catch (MalformedURLException e) {
          throw new GradleException("Malformed URL in classpath", e);
        }
      }).toArray(URL[]::new);
      this.urlClassLoader = URLClassLoader.newInstance(classpathUrls, getClass().getClassLoader());
      setClassLoader(urlClassLoader);
      setDependencies(classpathFiles);
      setSources(createScrSources(input));
    }

    @Override
    public void close() throws IOException {
      urlClassLoader.close();
    }

    private static Collection<Source> createScrSources(FileCollection input) {
      Collection<Source> sources = new ArrayList<>();

      input.getAsFileTree().matching(f -> f.include("**/*.class")).visit(new EmptyFileVisitor() {
        @Override
        public void visitFile(FileVisitDetails fileVisitDetails) {
          String dotSeparated = String.join(".", fileVisitDetails.getRelativePath().getSegments());
          String className = dotSeparated.substring(0, dotSeparated.length() - ".class".length());
          File file = fileVisitDetails.getFile();
          sources.add(new Source() {
            @Override
            public String getClassName() {
              return className;
            }

            @Override
            public File getFile() {
              return file;
            }
          });
        }
      });
      return sources;
    }
  }
}
