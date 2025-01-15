/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;
import org.ehcache.build.conventions.BaseConvention;
import org.ehcache.build.conventions.BndConvention;
import org.ehcache.build.conventions.JavaBaseConvention;
import org.ehcache.build.util.ConfigurationBucketSet;
import org.ehcache.build.util.OsgiManifestJarExtension;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
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
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static org.ehcache.build.plugins.VariantPlugin.COMMON_KIND;
import static org.ehcache.build.util.PluginUtils.camel;
import static org.ehcache.build.util.PluginUtils.kebab;
import static org.ehcache.build.util.PluginUtils.lower;
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

  private static final String CONTENTS_KIND = "contents";

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

    project.getPlugins().withType(VariantPlugin.class).configureEach(plugin -> {
      project.getExtensions().configure(VariantPlugin.VariantExtension.class, variants -> {
        variants.getVariants().configureEach(variant -> createVariantPackage(project, variant));
      });
    });

    project.getPlugins().withType(MavenPublishPlugin.class).configureEach(plugin -> {
      project.getExtensions().configure(PublishingExtension.class, publishing -> {
        publishing.getPublications().register("mavenJava", MavenPublication.class, mavenPublication -> {
          mavenPublication.from(javaComponent);
        });
      });
    });
  }

  private void createDefaultPackage(Project project) {
    createPackage(project, Optional.empty());
  }

  private void createVariantPackage(Project project, VariantPlugin.Variant variant) {
    createPackage(project, Optional.of(variant));
  }

  private void createPackage(Project project, Optional<VariantPlugin.Variant> variant) {
    String name = variant.map(VariantPlugin.Variant::getName).orElse(null);

    ServiceRegistry projectServices = ((ProjectInternal) project).getServices();
    JvmPluginServices jvmPluginServices = projectServices.get(JvmPluginServices.class);

    ConfigurationBucketSet packageBuckets = ConfigurationBucketSet.bucketsFor(project, name, CONTENTS_KIND);

    Configuration contentsCompileClasspath = jvmPluginServices.createResolvableConfiguration(lower(camel(name, CONTENTS_KIND, COMPILE_CLASSPATH_CONFIGURATION_NAME)), builder ->
      builder.extendsFrom(packageBuckets.bucket(CONTENTS_KIND)).requiresJavaLibrariesAPI());
    Configuration contentsRuntimeClasspath = jvmPluginServices.createResolvableConfiguration(lower(camel(name, CONTENTS_KIND, RUNTIME_CLASSPATH_CONFIGURATION_NAME)), builder ->
      builder.extendsFrom(packageBuckets.bucket(CONTENTS_KIND)).requiresJavaLibrariesRuntime());
    Configuration contentsSourcesFiles = jvmPluginServices.createResolvableConfiguration(lower(camel(name, CONTENTS_KIND, "sourcesFiles")), builder ->
      builder.extendsFrom(packageBuckets.bucket(CONTENTS_KIND)).requiresAttributes(refiner -> refiner.documentation(SOURCES)));

    TaskProvider<ShadowJar> shadowJar = project.getTasks().register(lower(camel(name, "jar")), ShadowJar.class, shadow -> {
      shadow.setGroup(BasePlugin.BUILD_GROUP);
      shadow.getArchiveClassifier().set(name);

      shadow.setConfigurations(Collections.singletonList(contentsRuntimeClasspath));
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

    Configuration apiElements = jvmPluginServices.createOutgoingElements(lower(camel(name, API_ELEMENTS_CONFIGURATION_NAME)), builder ->
      builder.extendsFrom(packageBuckets.api()).extendsFrom(packageBuckets.compileOnlyApi()).published().providesApi().artifact(shadowJar));
    configureDefaultTargetPlatform(apiElements, 8);
    Configuration compileClasspath = jvmPluginServices.createResolvableConfiguration(lower(camel(name, COMPILE_CLASSPATH_CONFIGURATION_NAME)), builder ->
      builder.extendsFrom(packageBuckets.implementation()).extendsFrom(packageBuckets.compileOnly()).requiresJavaLibrariesRuntime());
    Configuration runtimeElements = jvmPluginServices.createOutgoingElements(lower(camel(name, RUNTIME_ELEMENTS_CONFIGURATION_NAME)), builder ->
      builder.extendsFrom(packageBuckets.implementation()).extendsFrom(packageBuckets.runtimeOnly()).published().providesRuntime().artifact(shadowJar));
    configureDefaultTargetPlatform(runtimeElements, 8);
    Configuration runtimeClasspath = jvmPluginServices.createResolvableConfiguration(lower(camel(name, RUNTIME_CLASSPATH_CONFIGURATION_NAME)), builder ->
      builder.extendsFrom(packageBuckets.implementation()).extendsFrom(packageBuckets.runtimeOnly()).requiresJavaLibrariesRuntime());

    Provider<FileCollection> sourcesTree = project.provider(() -> contentsSourcesFiles.getResolvedConfiguration().getLenientConfiguration().getAllModuleDependencies().stream().flatMap(d -> d.getModuleArtifacts().stream())
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

    TaskProvider<Sync> sources = project.getTasks().register(lower(camel(name, "sources")), Sync.class, sync -> {
      sync.dependsOn(contentsSourcesFiles);
      sync.from(sourcesTree, spec -> spec.exclude("META-INF/**", "LICENSE", "NOTICE"));
      sync.into(project.getLayout().getBuildDirectory().dir(lower(camel(name, "sources"))));
    });
    TaskProvider<Jar> sourcesJar = project.getTasks().register(lower(camel(name, "sourcesJar")), Jar.class, jar -> {
      jar.setGroup(BasePlugin.BUILD_GROUP);
      jar.from(sources);
      jar.from(shadowJar, spec -> spec.include("META-INF/**", "LICENSE", "NOTICE"));
      jar.getArchiveClassifier().set(kebab(name, "sources"));
    });
    Configuration sourcesElements = jvmPluginServices.createOutgoingElements(lower(camel(name, SOURCES_ELEMENTS_CONFIGURATION_NAME)), builder ->
      builder.published().artifact(sourcesJar).providesAttributes(attributes -> attributes.documentation(SOURCES).asJar()));

    TaskProvider<Javadoc> javadoc = project.getTasks().register(lower(camel(name,  "javadoc")), Javadoc.class, task -> {
      task.setGroup(JavaBasePlugin.DOCUMENTATION_GROUP);
      task.setTitle(project.getName() + " " + project.getVersion() + " API");
      task.source(sources);
      task.include("**/*.java");
      task.setClasspath(contentsCompileClasspath.plus(compileClasspath));
      task.setDestinationDir(new File(project.getBuildDir(), "docs/" + lower(camel(name, "javadoc"))));
    });
    TaskProvider<Jar> javadocJar = project.getTasks().register(lower(camel(name, "javadocJar")), Jar.class, jar -> {
      jar.setGroup(BasePlugin.BUILD_GROUP);
      jar.from(javadoc);
      jar.getArchiveClassifier().set(kebab(name, "javadoc"));
    });
    Configuration javadocElements = jvmPluginServices.createOutgoingElements(lower(camel(name, JAVADOC_ELEMENTS_CONFIGURATION_NAME)), builder ->
      builder.published().artifact(javadocJar).providesAttributes(attributes -> attributes.documentation(JAVADOC).asJar()));

    shadowJar.configure(shadow -> {
      OsgiManifestJarExtension osgiExtension = new OsgiManifestJarExtension(shadow);
      osgiExtension.getClasspath().from(runtimeClasspath);
      osgiExtension.getSources().from(sources);
      BndConvention.bundleDefaults(project).execute(osgiExtension.getInstructions());
    });

    project.getComponents().named("java", AdhocComponentWithVariants.class, java -> {
      java.addVariantsFromConfiguration(apiElements, variantDetails -> {
        variantDetails.mapToMavenScope("compile");
        variant.ifPresent(v -> variantDetails.mapToOptional());
      });
      java.addVariantsFromConfiguration(runtimeElements, variantDetails -> {
        variantDetails.mapToMavenScope("runtime");
        variant.ifPresent(v -> variantDetails.mapToOptional());
      });
      java.addVariantsFromConfiguration(sourcesElements, variantDetails -> {});
      java.addVariantsFromConfiguration(javadocElements, variantDetails -> {});
    });

    if (variant.isPresent()) {
      VariantPlugin.Variant v = variant.get();
      ConfigurationBucketSet commonBuckets = ConfigurationBucketSet.bucketsFor(project, COMMON_KIND, CONTENTS_KIND);
      packageBuckets.extendFrom(commonBuckets);
      v.getConfigTraits().configureEach(trait -> packageBuckets.extendFrom(ConfigurationBucketSet.bucketsFor(project, camel(trait, "trait"))));
      v.getCapabilities().configureEach(capability -> {
        apiElements.getOutgoing().capability(capability);
        runtimeElements.getOutgoing().capability(capability);
        sourcesElements.getOutgoing().capability(capability);
        javadocElements.getOutgoing().capability(capability);
      });
    } else {
      project.getPlugins().withType(VariantPlugin.class).configureEach(variantPlugin -> {
        project.getExtensions().configure(VariantPlugin.VariantExtension.class, variants -> {
          VariantPlugin.Variant v = variants.getDefaultVariant();
          ConfigurationBucketSet commonBuckets = ConfigurationBucketSet.bucketsFor(project, COMMON_KIND, CONTENTS_KIND);
          packageBuckets.extendFrom(commonBuckets);
          v.getConfigTraits().configureEach(trait -> packageBuckets.extendFrom(ConfigurationBucketSet.bucketsFor(project, camel(trait, "trait"))));
          v.getCapabilities().configureEach(capability -> {
            apiElements.getOutgoing().capability(capability);
            runtimeElements.getOutgoing().capability(capability);
            sourcesElements.getOutgoing().capability(capability);
            javadocElements.getOutgoing().capability(capability);
          });
        });
      });
    }

    project.getTasks().named(LifecycleBasePlugin.ASSEMBLE_TASK_NAME).configure(task -> {
      task.dependsOn(shadowJar);
      task.dependsOn(javadocJar);
      task.dependsOn(sourcesJar);
    });
  }
}
