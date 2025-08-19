package org.ehcache.build.conventions;

import aQute.bnd.gradle.BndBuilderPlugin;
import aQute.bnd.gradle.BundleTaskExtension;
import aQute.bnd.osgi.Constants;
import org.ehcache.build.plugins.osgids.OsgiDsPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ExternalDependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPom;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.plugins.PublishingPlugin;
import org.gradle.api.tasks.bundling.Jar;

import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

public class BndConvention implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(BndBuilderPlugin.class);
    project.getPlugins().apply(OsgiDsPlugin.class);
    project.getPlugins().apply(DeployConvention.class);

    project.getTasks().named(JavaPlugin.JAR_TASK_NAME, Jar.class, jar -> {
      jar.getExtensions().configure(BundleTaskExtension.class, bundle -> configureBundleDefaults(project, bundle));
    });

    project.getConfigurations().named("baseline", config -> {
      config.getResolutionStrategy().getComponentSelection().all(selection -> {
        if (!selection.getCandidate().getVersion().matches("^\\d+(?:\\.\\d+)*$")) {
          selection.reject("Only full releases can be used as OSGi baselines");
        }
      });
    });

    String dependencyNotation = project.getGroup() + ":" + project.getName() + ":(," + project.getVersion() + "[";
    Dependency baseline = project.getDependencies().add("baseline", dependencyNotation);
    if (baseline instanceof ProjectDependency) {
      throw new GradleException("Baseline should not be a project dependency");
    } else if (baseline instanceof ExternalDependency) {
      ((ExternalDependency) baseline).setForce(true);
      ((ExternalDependency) baseline).setTransitive(false);
    } else {
      throw new IllegalArgumentException("Unexpected dependency type: " + baseline);
    }
  }

  public static void configureBundleDefaults(Project project, BundleTaskExtension bundle) {
    MapProperty<String, String> defaultInstructions = project.getObjects().mapProperty(String.class, String.class);
    bundleDefaults(project).execute(defaultInstructions);
    bundle.bnd(defaultInstructions.map(kv -> kv.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(joining(lineSeparator()))));
  }

  public static Action<MapProperty<String, String>> bundleDefaults(Project project) {
    return properties -> {
      project.getPlugins().withType(PublishingPlugin.class).configureEach(publishingPlugin -> {
        project.getExtensions().getByType(PublishingExtension.class).getPublications().withType(MavenPublication.class).stream().findAny().ifPresent(publication -> {
          properties.put(Constants.BUNDLE_NAME, publication.getPom().getName());
          properties.put(Constants.BUNDLE_DESCRIPTION, publication.getPom().getDescription());
        });
      });
      properties.put(Constants.AUTOMATIC_MODULE_NAME, "org." + project.getName().replace('-','.'));
      properties.put(Constants.BUNDLE_SYMBOLICNAME, project.getGroup() + "." + project.getName());
      properties.put(Constants.BUNDLE_DOCURL, "http://ehcache.org");
      properties.put(Constants.BUNDLE_LICENSE, "LICENSE");
      properties.put(Constants.BUNDLE_VENDOR, "IBM Corp.");
      properties.put(Constants.BUNDLE_VERSION, osgiFixedVersion(project.getVersion().toString()));
      properties.put(Constants.SERVICE_COMPONENT, "OSGI-INF/*.xml");
    };
  }

  public static String osgiFixedVersion(String version) {
    /*
     * The bnd gradle plugin does not handle our 2-digit snapshot versioning scheme very well. It maps `x.y-SNAPSHOT`
     * to `x.y.0.SNAPSHOT`. This is bad since `x.y.0.SNAPSHOT` is considered to be less than *all* `x.y.z`. This means
     * the baseline version range expression `(,x.y.0.SNAPSHOT[` will always pick the last release from the previous
     * minor line. To fix this we manually map to a 3-digit snapshot version where the 3rd digit is a number chosen
     * to be higher than we would ever release ('All the worlds a VAX').
     */
    return version.replaceAll("^(\\d+.\\d+)-SNAPSHOT$", "$1.999-SNAPSHOT");
  }
}
