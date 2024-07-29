package org.ehcache.build.conventions;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.internal.artifacts.ivyservice.projectmodule.ProjectComponentPublication;
import org.gradle.api.internal.component.SoftwareComponentInternal;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.internal.publication.MavenPomInternal;
import org.gradle.api.publish.maven.internal.publisher.MavenProjectIdentity;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.publish.maven.tasks.AbstractPublishToMaven;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.WriteProperties;
import org.gradle.jvm.tasks.Jar;
import org.gradle.plugins.signing.SigningExtension;
import org.gradle.plugins.signing.SigningPlugin;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static org.gradle.api.publish.plugins.PublishingPlugin.PUBLISH_TASK_GROUP;

/**
 * Deploy plugin for published artifacts. This is an abstraction over the {@code maven-publish} plugin.
 * </pre>
 * Defaults:
 * <ul>
 *   <li>POM: population of general content: organization, issue-management, scm, etc.</li>
 *   <li>POM copied to {@code META-INF/maven/groupId/artifactId/pom.xml}</li>
 *   <li>POM properties file copied to {@code META-INF/maven/groupId/artifactId/pom.properties}</li>
 *   <li>Javadoc and Source JAR Publishing</li>
 *   <li>{@code install} as alias of {@code publishToMavenLocal}</li>
 * </ul>
 */
public class DeployConvention implements Plugin<Project> {

  private static final Predicate<Project> IS_RELEASE = p -> !p.getVersion().toString().endsWith("-SNAPSHOT");

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(SigningPlugin.class);
    project.getPlugins().apply(MavenPublishPlugin.class);

    project.getExtensions().configure(PublishingExtension.class, publishing -> {
      publishing.getPublications().withType(MavenPublication.class).configureEach(mavenPublication -> mavenPublication.pom(pom -> {
        pom.getUrl().set("http://ehcache.org");
        pom.organization(org -> {
          org.getName().set("Super iPaaS Integration LLC, an IBM Company");
          org.getUrl().set("http://terracotta.org");
        });
        pom.issueManagement(issue -> {
          issue.getSystem().set("Github");
          issue.getUrl().set("https://github.com/ehcache/ehcache3/issues");
        });
        pom.scm(scm -> {
          scm.getUrl().set("https://github.com/ehcache/ehcache3");
          scm.getConnection().set("scm:git:https://github.com/ehcache/ehcache3.git");
          scm.getDeveloperConnection().set("scm:git:git@github.com:ehcache/ehcache3.git");
        });
        pom.licenses(licenses -> licenses.license(license -> {
          license.getName().set("The Apache Software License, Version 2.0");
          license.getUrl().set("http://www.apache.org/licenses/LICENSE-2.0.txt");
          license.getDistribution().set("repo");
        }));
        pom.developers(devs -> devs.developer(dev -> {
          dev.getName().set("Terracotta Engineers");
          dev.getEmail().set("tc-oss-dg@ibm.com");
          dev.getOrganization().set("Super iPaaS Integration LLC, an IBM Company");
          dev.getOrganizationUrl().set("http://ehcache.org");
        }));
      }));
    });

    project.getExtensions().configure(SigningExtension.class, signing -> {
      signing.setRequired((Callable<Boolean>) () -> IS_RELEASE.test(project) && project.getGradle().getTaskGraph().getAllTasks().stream().anyMatch(t -> t instanceof PublishToMavenRepository));
      signing.sign(project.getExtensions().getByType(PublishingExtension.class).getPublications());
    });

    /*
     * Do **not** convert the anonymous Action here to a lambda expression - it will break Gradle's up-to-date tracking
     * and cause tasks to be needlessly rerun.
     */
    //noinspection Convert2Lambda
    project.getTasks().withType(AbstractPublishToMaven.class).configureEach(publishTask -> publishTask.doFirst(new Action<Task>() {
      @Override
      public void execute(Task task) {
        MavenPublication publication = publishTask.getPublication();
        if (publication instanceof ProjectComponentPublication) {
          SoftwareComponentInternal component = ((ProjectComponentPublication) publication).getComponent();
          if (component != null) { //The shadow plugin doesn"t associate a component with the publication
            Collection<ProjectDependency> unpublishedDeps = component.getUsages().stream().flatMap(usage ->
              usage.getDependencies().stream().filter(ProjectDependency.class::isInstance).map(ProjectDependency.class::cast).filter(moduleDependency ->
                !moduleDependency.getDependencyProject().getPlugins().hasPlugin(DeployConvention.class))).collect(toList());
            if (!unpublishedDeps.isEmpty()) {
              project.getLogger().warn("{} has applied the deploy plugin but has unpublished project dependencies: {}", project, unpublishedDeps);
            }
          }
        }
      }
    }));

    project.getTasks().register("install", task ->
      task.dependsOn(project.getTasks().named(MavenPublishPlugin.PUBLISH_LOCAL_LIFECYCLE_TASK_NAME))
    );

    project.getPlugins().withType(JavaPlugin.class).configureEach(plugin -> {
      project.getExtensions().configure(JavaPluginExtension.class, java -> {
        java.withJavadocJar();
        java.withSourcesJar();
      });

      project.afterEvaluate(p -> {
        p.getExtensions().configure(PublishingExtension.class, publishing -> {
          if (publishing.getPublications().isEmpty()) {
            publishing.publications(publications -> publications.register("mavenJava", MavenPublication.class, mavenJava -> mavenJava.from(p.getComponents().getByName("java"))));
          }
        });

        p.getTasks().withType(GenerateMavenPom.class).all(pomTask -> {
          MavenProjectIdentity identity = ((MavenPomInternal) pomTask.getPom()).getProjectIdentity();
          TaskProvider<WriteProperties> pomPropertiesTask = project.getTasks().register(pomTask.getName().replace("PomFile", "PomProperties"), WriteProperties.class, task -> {
            task.dependsOn(pomTask);
            task.setGroup(PUBLISH_TASK_GROUP);
            task.setOutputFile(new File(pomTask.getDestination().getParentFile(), "pom.properties"));
            task.property("groupId", identity.getGroupId());
            task.property("artifactId", identity.getArtifactId());
            task.property("version", identity.getVersion());
          });

          project.getTasks().withType(Jar.class).configureEach(jar -> {
            jar.into("META-INF/maven/" + identity.getGroupId().get() + "/" + identity.getArtifactId().get(), spec -> {
              spec.from(pomTask, pom -> pom.rename(".*", "pom.xml"));
              spec.from(pomPropertiesTask);
            });
          });
        });
      });
    });
  }
}
