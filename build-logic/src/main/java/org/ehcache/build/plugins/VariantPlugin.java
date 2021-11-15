package org.ehcache.build.plugins;

import aQute.bnd.gradle.BndBuilderPlugin;
import aQute.bnd.gradle.BundleTaskExtension;
import org.ehcache.build.conventions.BndConvention;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.capabilities.Capability;
import org.gradle.api.file.Directory;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.internal.HasConvention;
import org.gradle.api.internal.project.ProjectInternal;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.plugins.jvm.internal.JvmPluginServices;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.internal.component.external.model.ImmutableCapability;
import org.unbrokendome.gradle.plugins.xjc.XjcPlugin;
import org.unbrokendome.gradle.plugins.xjc.XjcSourceSetConvention;

import java.util.Locale;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.gradle.api.attributes.DocsType.SOURCES;
import static org.gradle.api.plugins.JavaPlugin.SOURCES_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.tasks.SourceSet.MAIN_SOURCE_SET_NAME;

public class VariantPlugin implements Plugin<Project> {

  private static final String COMMON_SOURCE_SET_NAME = "common";

  @Override
  public void apply(Project project) {
    VariantExtension variants = project.getExtensions().create("variants", VariantExtension.class, project);
    configureJavaPluginBehavior(project, variants);
  }

  private void configureJavaPluginBehavior(Project project, VariantExtension variants) {
    project.getPlugins().withType(JavaPlugin.class, javaPlugin ->  {
      JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);

      variants.getVariants().all(variant -> {
        if (variant.hasSources().get()) {
          SourceSet commonSources = java.getSourceSets().findByName(COMMON_SOURCE_SET_NAME);
          if (commonSources == null) {
            commonSources = java.getSourceSets().create(COMMON_SOURCE_SET_NAME, common -> {
              project.getTasks().named(common.getCompileJavaTaskName(), task -> task.setEnabled(false));
              project.getTasks().named(common.getClassesTaskName(), task -> task.setEnabled(false));
              linkToCommonSource(project, common, java.getSourceSets().getByName(MAIN_SOURCE_SET_NAME));
            });
          }
          SourceSet variantSources = java.getSourceSets().create(variant.getName());

          linkToCommonSource(project, commonSources, variantSources);

          java.registerFeature(variant.getName(), feature -> {
            feature.usingSourceSet(variantSources);
            feature.withSourcesJar();
            variant.getCapabilities().get().forEach(capability -> {
              feature.capability(capability.getGroup(), capability.getName(), requireNonNull(capability.getVersion()));
            });
          });

          project.getPlugins().withType(BndBuilderPlugin.class, bnd -> {
            project.getTasks().named(variantSources.getJarTaskName(), Jar.class, jar -> {
              jar.setDescription("Assembles a bundle containing the " + variant + " variant classes.");
              BundleTaskExtension extension = jar.getExtensions().create(BundleTaskExtension.NAME, BundleTaskExtension.class, jar);
              BndConvention.configureBundleDefaults(project, extension);
              jar.doLast("buildBundle", extension.buildAction());
            });
          });
        } else {
          SourceSet mainSource = java.getSourceSets().getByName(MAIN_SOURCE_SET_NAME);

          JvmPluginServices jvmPluginServices = ((ProjectInternal) project).getServices().get(JvmPluginServices.class);

          Configuration api = bucket(project, "api", variant.getName());
          Configuration implementation = bucket(project, "implementation", variant.getName()).extendsFrom(api);
          Configuration compileOnlyApi = bucket(project, "CompileOnlyApi", variant.getName());
          Configuration runtimeOnly = bucket(project, "RuntimeOnly", variant.getName());

          Configuration apiElements = jvmPluginServices.createOutgoingElements(variant + "ApiElements", builder ->
            builder.fromSourceSet(mainSource).withCapabilities(variant.getCapabilities().get())
              .extendsFrom(api, compileOnlyApi).withClassDirectoryVariant().providesApi());
          project.getConfigurations().named(mainSource.getApiElementsConfigurationName(),
            config -> config.getOutgoing().getArtifacts().all(artifact -> apiElements.getOutgoing().getArtifacts().add(artifact)));

          Configuration runtimeElements = jvmPluginServices.createOutgoingElements(variant + "RuntimeElements", builder ->
            builder.fromSourceSet(mainSource).withCapabilities(variant.getCapabilities().get()).published()
              .extendsFrom(implementation, runtimeOnly).providesRuntime());
          project.getConfigurations().named(mainSource.getRuntimeElementsConfigurationName(),
            config -> config.getOutgoing().getArtifacts().all(artifact -> runtimeElements.getOutgoing().getArtifacts().add(artifact)));

          Configuration sourcesElements = jvmPluginServices.createOutgoingElements(variant + capitalize(SOURCES_ELEMENTS_CONFIGURATION_NAME), builder ->
            builder.fromSourceSet(mainSource).withCapabilities(variant.getCapabilities().get()).published()
              .providesAttributes(attributes -> attributes.documentation(SOURCES).asJar()));
          project.getConfigurations().named(mainSource.getSourcesElementsConfigurationName(),
            config -> config.getOutgoing().getArtifacts().all(artifact -> sourcesElements.getOutgoing().getArtifacts().add(artifact)));
        }
      });
    });
  }

  private Configuration bucket(Project project, String kind, String variant) {
    Configuration configuration = project.getConfigurations().maybeCreate(variant + capitalize(kind));
    configuration.setDescription(capitalize(kind) + " dependencies for " + variant);
    configuration.setVisible(false);
    configuration.setCanBeResolved(false);
    configuration.setCanBeConsumed(false);
    return configuration;
  }

  private static String capitalize(String word) {
    return word.substring(0, 1).toUpperCase(Locale.ROOT) + word.substring(1);
  }

  private static void linkToCommonSource(Project project, SourceSet commonSources, SourceSet derivedSources) {
    registerCommonCopyTask(project, commonSources, derivedSources, SourceSet::getJava);
    registerCommonCopyTask(project, commonSources, derivedSources, SourceSet::getResources);

    Configuration commonApi = project.getConfigurations().maybeCreate(commonSources.getApiConfigurationName());
    project.getConfigurations().maybeCreate(derivedSources.getApiConfigurationName()).extendsFrom(commonApi);
    Configuration commonImplementation = project.getConfigurations().maybeCreate(commonSources.getImplementationConfigurationName());
    project.getConfigurations().maybeCreate(derivedSources.getImplementationConfigurationName()).extendsFrom(commonImplementation);

    project.getPlugins().withType(XjcPlugin.class, plugin -> {
      Function<SourceSet, XjcSourceSetConvention> xjc = sourceSet -> ((HasConvention) sourceSet).getConvention().getPlugin(XjcSourceSetConvention.class);

      XjcSourceSetConvention commonXjc = xjc.apply(commonSources);
      project.getTasks().named(commonXjc.getXjcGenerateTaskName(), task -> task.setEnabled(false));

      registerCommonCopyTask(project, commonSources, derivedSources, xjc.andThen(XjcSourceSetConvention::getXjcSchema));
      registerCommonCopyTask(project, commonSources, derivedSources, xjc.andThen(XjcSourceSetConvention::getXjcCatalog));
      registerCommonCopyTask(project, commonSources, derivedSources, xjc.andThen(XjcSourceSetConvention::getXjcBinding));
      registerCommonCopyTask(project, commonSources, derivedSources, xjc.andThen(XjcSourceSetConvention::getXjcUrl));
    });
  }

  private static void registerCommonCopyTask(Project project, SourceSet common, SourceSet variant, Function<SourceSet, SourceDirectorySet> type) {
    SourceDirectorySet commonSource = type.apply(common);
    Provider<Directory> variantLocation = project.getLayout().getBuildDirectory().dir("generated/resources/common/" + variant.getName() + "/" + commonSource.getName());
    TaskProvider<Sync> variantTask = project.getTasks().register(variant.getTaskName("copyCommon", commonSource.getName()), Sync.class, sync -> {
      sync.from(commonSource);
      sync.into(variantLocation);
    });
    type.apply(variant).srcDir(variantTask);
  }

  public static class Variant {

    private final String name;
    private final Property<Boolean> hasSources;
    private final ListProperty<Capability> capabilities;

    public Variant(String name, ObjectFactory objectFactory) {
      this.name = name;
      this.hasSources = objectFactory.property(Boolean.class).convention(false);
      this.capabilities = objectFactory.listProperty(Capability.class);

      this.hasSources.finalizeValueOnRead();
      this.capabilities.finalizeValueOnRead();
    }

    public String getName() {
      return name;
    }

    public Property<Boolean> hasSources() {
      return hasSources;
    }

    public ListProperty<Capability> getCapabilities() {
      return capabilities;
    }

    public void withSeparateSource() {
      this.hasSources.set(true);
    }

    public void capability(String group, String name, String version) {
      this.capabilities.add(new ImmutableCapability(group, name, version));
    }
  }

  public static class VariantExtension {

    private final ObjectFactory objectFactory;
    private final NamedDomainObjectContainer<Variant> variants;

    public VariantExtension(Project project) {
      this.objectFactory = project.getObjects();
      this.variants = project.container(Variant.class);
    }

    public void variant(String variant, Action<Variant> action) {
      Variant v = new Variant(variant, objectFactory);
      action.execute(v);
      variants.add(v);
    }

    public NamedDomainObjectContainer<Variant> getVariants() {
      return variants;
    }
  }
}
