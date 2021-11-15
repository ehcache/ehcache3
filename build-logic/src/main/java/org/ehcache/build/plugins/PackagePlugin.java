/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.build.plugins;

import aQute.bnd.gradle.BundleTaskExtension;
import aQute.bnd.osgi.Constants;
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;
import org.ehcache.build.conventions.BaseConvention;
import org.ehcache.build.conventions.BndConvention;
import org.ehcache.build.conventions.JavaBaseConvention;
import org.ehcache.build.util.OsgiManifestJarExtension;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.capabilities.Capability;
import org.gradle.api.component.AdhocComponentWithVariants;
import org.gradle.api.component.SoftwareComponentFactory;
import org.gradle.api.file.DuplicatesStrategy;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.internal.project.ProjectInternal;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.jvm.internal.JvmPluginServices;
import org.gradle.api.provider.Provider;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.internal.jvm.Jvm;
import org.gradle.internal.resolve.ArtifactResolveException;
import org.gradle.internal.service.ServiceRegistry;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static org.gradle.api.attributes.DocsType.JAVADOC;
import static org.gradle.api.attributes.DocsType.SOURCES;
import static org.gradle.api.attributes.DocsType.USER_MANUAL;
import static org.gradle.api.internal.artifacts.JavaEcosystemSupport.configureDefaultTargetPlatform;
import static org.gradle.api.plugins.JavaPlugin.API_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.JAVADOC_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.SOURCES_ELEMENTS_CONFIGURATION_NAME;

/**
 * EhDistribute
 */
public class PackagePlugin implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    project.getPlugins().apply(BaseConvention.class);
    project.getPlugins().apply(JavaBaseConvention.class);

    ServiceRegistry projectServices = ((ProjectInternal) project).getServices();
    JvmPluginServices jvmPluginServices = projectServices.get(JvmPluginServices.class);
    SoftwareComponentFactory softwareComponentFactory = projectServices.get(SoftwareComponentFactory.class);
    AdhocComponentWithVariants javaComponent = softwareComponentFactory.adhoc("java");
    project.getComponents().add(javaComponent);

    TaskProvider<Zip> asciidocZip = project.getTasks().register("asciidocZip", Zip.class, zip -> {
      zip.getArchiveClassifier().set("docs");
      zip.from(project.getTasks().getByPath(":docs:userDoc"));
    });
    Configuration userdocElements = jvmPluginServices.createOutgoingElements("userdocElements", builder ->
      builder.published().artifact(asciidocZip).providesAttributes(attributes -> attributes.documentation(USER_MANUAL)));
    javaComponent.addVariantsFromConfiguration(userdocElements, variantDetails -> {});

    createDefaultPackage(project);

    project.getPlugins().withType(VariantPlugin.class, plugin -> {
      project.getExtensions().configure(VariantPlugin.VariantExtension.class, variants -> {
        variants.getVariants().all(variant -> {
          createPackage(project, variant.getName(), variant.getCapabilities().get());
        });
      });
    });

    project.getPlugins().withType(MavenPublishPlugin.class, plugin -> {
      project.getExtensions().configure(PublishingExtension.class, publishing -> {
        publishing.getPublications().register("mavenJava", MavenPublication.class, mavenPublication -> {
          mavenPublication.from(javaComponent);
        });
      });
    });
  }

  private void createDefaultPackage(Project project) {
    createPackage(project, null, emptyList());
  }

  private void createPackage(Project project, String variant, List<Capability> capabilities) {
    ServiceRegistry projectServices = ((ProjectInternal) project).getServices();
    JvmPluginServices jvmPluginServices = projectServices.get(JvmPluginServices.class);

    Configuration contents = project.getConfigurations().create(camelPrefix(variant, "contents"), config -> config.setTransitive(true));
    Configuration contentsSources = jvmPluginServices.createResolvableConfiguration(camelPrefix(variant, "sources"), builder ->
      builder.extendsFrom(contents).requiresAttributes(refiner -> refiner.documentation(SOURCES)));

    TaskProvider<ShadowJar> shadowJar = project.getTasks().register(camelPrefix(variant, "jar"), ShadowJar.class, shadow -> {
      shadow.setGroup(BasePlugin.BUILD_GROUP);
      shadow.getArchiveClassifier().set(variant);

      shadow.setConfigurations(Collections.singletonList(contents));
      shadow.relocate("org.terracotta.statistics.", "org.ehcache.shadow.org.terracotta.statistics.");
      shadow.relocate("org.terracotta.offheapstore.", "org.ehcache.shadow.org.terracotta.offheapstore.");
      shadow.relocate("org.terracotta.context.", "org.ehcache.shadow.org.terracotta.context.");
      shadow.relocate("org.terracotta.utilities.", "org.ehcache.shadow.org.terracotta.utilities.");

      shadow.mergeServiceFiles();

      shadow.exclude("META-INF/MANIFEST.MF", "LICENSE", "NOTICE");

      // LICENSE is included in root gradle build
      shadow.from(new File(project.getRootDir(), "NOTICE"));
      shadow.setDuplicatesStrategy(DuplicatesStrategy.EXCLUDE);
    });

    Provider<FileCollection> sourcesTree = project.provider(() -> contentsSources.getResolvedConfiguration().getLenientConfiguration().getAllModuleDependencies().stream().flatMap(d -> d.getModuleArtifacts().stream())
      .map(artifact -> {
        try {
          return Optional.of(artifact.getFile());
        } catch (ArtifactResolveException e) {
          return Optional.<File>empty();
        }
      }).filter(Optional::isPresent).map(Optional::get).distinct().map(file -> {
        if (file.isFile()) {
          return project.zipTree(file);
        } else {
          return project.fileTree(file);
        }
      }).reduce(FileTree::plus).orElse(project.files().getAsFileTree()));

    TaskProvider<Sync> sources = project.getTasks().register(camelPrefix(variant, "sources"), Sync.class, sync -> {
      sync.dependsOn(contentsSources);
      sync.from(sourcesTree, spec -> spec.exclude("META-INF/**", "LICENSE", "NOTICE"));
      sync.into(project.getLayout().getBuildDirectory().dir(camelPrefix(variant,"sources")));
    });

    TaskProvider<Jar> sourcesJar = project.getTasks().register(camelPrefix(variant, "sourcesJar"), Jar.class, jar -> {
      jar.setGroup(BasePlugin.BUILD_GROUP);
      jar.from(sources);
      jar.from(shadowJar, spec -> spec.include("META-INF/**", "LICENSE", "NOTICE"));
      jar.getArchiveClassifier().set(kebabPrefix(variant, "sources"));
    });

    TaskProvider<Javadoc> javadoc = project.getTasks().register(camelPrefix(variant,  "javadoc"), Javadoc.class, task -> {
      task.setGroup(JavaBasePlugin.DOCUMENTATION_GROUP);
      task.setTitle(project.getName() + " " + project.getVersion() + " API");
      task.source(sources);
      task.include("*.java");
      task.setClasspath(contents);
      task.setDestinationDir(new File(project.getBuildDir(), "docs/" + camelPrefix(variant, "javadoc")));
    });
    TaskProvider<Jar> javadocJar = project.getTasks().register(camelPrefix(variant, "javadocJar"), Jar.class, jar -> {
      jar.setGroup(BasePlugin.BUILD_GROUP);
      jar.from(javadoc);
      jar.getArchiveClassifier().set(kebabPrefix(variant, "javadoc"));
    });

    Configuration api = bucket(project, camelPrefix(variant, "api"));
    Configuration implementation = bucket(project, camelPrefix(variant, "implementation")).extendsFrom(api);
    Configuration compileOnlyApi = bucket(project, camelPrefix(variant, "compileOnlyApi"));
    Configuration runtimeOnly = bucket(project, camelPrefix(variant, "runtimeOnly"));

    Configuration apiElements = jvmPluginServices.createOutgoingElements(camelPrefix(variant, API_ELEMENTS_CONFIGURATION_NAME), builder ->
      builder.extendsFrom(api, compileOnlyApi).published().providesApi().withCapabilities(capabilities).artifact(shadowJar));
    configureDefaultTargetPlatform(apiElements, parseInt(Jvm.current().getJavaVersion().getMajorVersion()));
    Configuration compileClasspath = jvmPluginServices.createResolvableConfiguration(camelPrefix(variant, COMPILE_CLASSPATH_CONFIGURATION_NAME), builder ->
      builder.extendsFrom(apiElements).requiresJavaLibrariesRuntime());
    Configuration runtimeElements = jvmPluginServices.createOutgoingElements(camelPrefix(variant, RUNTIME_ELEMENTS_CONFIGURATION_NAME), builder ->
      builder.extendsFrom(implementation, runtimeOnly).published().providesRuntime().withCapabilities(capabilities).artifact(shadowJar));
    configureDefaultTargetPlatform(runtimeElements, parseInt(Jvm.current().getJavaVersion().getMajorVersion()));
    Configuration runtimeClasspath = jvmPluginServices.createResolvableConfiguration(camelPrefix(variant, RUNTIME_CLASSPATH_CONFIGURATION_NAME), builder ->
      builder.extendsFrom(runtimeElements).requiresJavaLibrariesRuntime());

    Configuration sourcesElements = jvmPluginServices.createOutgoingElements(camelPrefix(variant, SOURCES_ELEMENTS_CONFIGURATION_NAME), builder ->
      builder.published().artifact(sourcesJar).withCapabilities(capabilities).providesAttributes(attributes -> attributes.documentation(SOURCES).asJar()));
    Configuration javadocElements = jvmPluginServices.createOutgoingElements(camelPrefix(variant, JAVADOC_ELEMENTS_CONFIGURATION_NAME), builder ->
      builder.published().artifact(javadocJar).withCapabilities(capabilities).providesAttributes(attributes -> attributes.documentation(JAVADOC).asJar()));

    shadowJar.configure(shadow -> {
      OsgiManifestJarExtension osgiExtension = new OsgiManifestJarExtension(shadow);
      osgiExtension.getClasspath().from(runtimeClasspath);
      osgiExtension.getSources().from(sources);
      BndConvention.bundleDefaults(project).execute(osgiExtension.getInstructions());
    });

    project.getComponents().named("java", AdhocComponentWithVariants.class, java -> {
      java.addVariantsFromConfiguration(apiElements, variantDetails -> {
        variantDetails.mapToMavenScope("compile");
        if (variant != null) {
          variantDetails.mapToOptional();
        }
      });
      java.addVariantsFromConfiguration(runtimeElements, variantDetails -> {
        variantDetails.mapToMavenScope("runtime");
        if (variant != null) {
          variantDetails.mapToOptional();
        }
      });
      java.addVariantsFromConfiguration(sourcesElements, variantDetails -> {});
      java.addVariantsFromConfiguration(javadocElements, variantDetails -> {});
    });
  }

  private Configuration bucket(Project project, String kind) {
    Configuration configuration = project.getConfigurations().maybeCreate(kind);
    configuration.setDescription(capitalize(kind) + " dependencies");
    configuration.setVisible(false);
    configuration.setCanBeResolved(false);
    configuration.setCanBeConsumed(false);
    return configuration;
  }

  private static String camelPrefix(String variant, String thing) {
    if (variant == null) {
      return thing;
    } else {
      return variant + capitalize(thing);
    }
  }

  private static String kebabPrefix(String variant, String thing) {
    if (variant == null) {
      return thing;
    } else {
      return variant + "-" + thing;
    }
  }

  private static String capitalize(String word) {
    return word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1);
  }
}
