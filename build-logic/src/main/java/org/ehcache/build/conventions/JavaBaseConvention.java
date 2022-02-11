package org.ehcache.build.conventions;

import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.java.archives.Attributes;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.api.tasks.testing.Test;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.internal.jvm.JavaInfo;
import org.gradle.internal.jvm.Jvm;
import org.gradle.process.internal.ExecException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;

public class JavaBaseConvention implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project.getPlugins().apply(JavaBasePlugin.class);
    project.getPlugins().apply(BaseConvention.class);

    JavaInfo testJava = fetchTestJava(project);
    project.getExtensions().getExtraProperties().set("testJava", testJava);

    project.getExtensions().configure(JavaPluginExtension.class, java -> {
      java.setSourceCompatibility(JavaVersion.VERSION_11);
      java.setTargetCompatibility(JavaVersion.VERSION_11);
    });

    project.getTasks().withType(Jar.class).configureEach(jar -> {
      jar.manifest(manifest -> {
        Attributes attributes = manifest.getAttributes();
        attributes.put("Implementation-Title", project.getName());
        attributes.put("Implementation-Vendor-Id", project.getGroup());
        attributes.put("Implementation-Version", project.getVersion());
        attributes.put("Implementation-Revision", getRevision(project));
        attributes.put("Built-By", System.getProperty("user.name"));
        attributes.put("Built-JDK", System.getProperty("java.version"));
      });
      jar.from(project.getRootProject().file("LICENSE"));
    });

    project.getTasks().withType(Test.class).configureEach(test -> {
      test.setExecutable(testJava.getJavaExecutable());
      test.setMaxHeapSize("256m");
      test.setMaxParallelForks(16);
      test.systemProperty("java.awt.headless", "true");
    });

    project.getTasks().withType(JavaCompile.class).configureEach(compile -> {
      compile.getOptions().setEncoding("UTF-8");
      compile.getOptions().setCompilerArgs(asList("-Werror", "-Xlint:all"));
    });

    project.getTasks().withType(Javadoc.class).configureEach(javadoc -> {
      javadoc.setTitle(project.getName() + " " + project.getVersion() + " API");
      javadoc.exclude("**/internal/**");
      javadoc.getOptions().setEncoding("UTF-8");
      ((CoreJavadocOptions) javadoc.getOptions()).addStringOption("Xdoclint:none", "-quiet");
    });
  }

  private static JavaInfo fetchTestJava(Project project) {
    Object testVM = project.findProperty("testVM");
    if (testVM == null) {
      return Jvm.current();
    } else {
      File jvmHome = project.file(testVM);
      if (!jvmHome.exists() && project.hasProperty(testVM.toString())) {
        testVM = project.property(testVM.toString());
        jvmHome = project.file(testVM);
      }

      return jvmForHome(project, jvmHome);
    }
  }

  private static final Pattern VERSION_OUTPUT = Pattern.compile("\\w+ version \"(?<version>.+)\"");
  private static Jvm jvmForHome(Project project, File home) {
    File java = Jvm.forHome(home).getJavaExecutable();

    OutputStream stdout = new ByteArrayOutputStream();
    OutputStream stderr = new ByteArrayOutputStream();
    project.exec(spec -> {
      spec.executable(java);
      spec.args("-version");
      spec.setStandardOutput(stdout);
      spec.setErrorOutput(stderr);
    });
    String versionOutput = stderr.toString();
    Matcher matcher = VERSION_OUTPUT.matcher(versionOutput);
    if (matcher.find()) {
      return Jvm.discovered(home, null, JavaVersion.toVersion(matcher.group("version")));
    } else {
      throw new IllegalArgumentException("Could not parse version of " + java + " from output:\n" + versionOutput);
    }
  }


  private static Object getRevision(Project project) {
    String envCommit = System.getenv("GIT_COMMIT");
    if(envCommit != null) {
      return envCommit;
    } else {
      try {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        project.exec(spec -> {
          spec.executable("git");
          spec.args("rev-parse", "HEAD");
          spec.setStandardOutput(stdout);
          spec.setErrorOutput(stderr);
        }).assertNormalExitValue();

        return stdout.toString().trim();
      } catch (ExecException e) {
        return "Unknown";
      }
    }
  }
}
