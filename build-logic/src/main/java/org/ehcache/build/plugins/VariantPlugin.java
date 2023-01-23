package org.ehcache.build.plugins;

import aQute.bnd.gradle.BndBuilderPlugin;
import aQute.bnd.gradle.BundleTaskExtension;
import org.ehcache.build.conventions.BndConvention;
import org.ehcache.build.util.ConfigurationBucketSet;
import org.gradle.api.Action;
import org.gradle.api.DomainObjectSet;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.NamedDomainObjectProvider;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.capabilities.Capability;
import org.gradle.api.file.Directory;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.internal.HasConvention;
import org.gradle.api.internal.artifacts.dsl.CapabilityNotationParserFactory;
import org.gradle.api.internal.project.ProjectInternal;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.plugins.jvm.internal.JvmPluginServices;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.internal.typeconversion.NotationParser;
import org.gradle.language.base.plugins.LifecycleBasePlugin;
import org.unbrokendome.gradle.plugins.xjc.XjcPlugin;
import org.unbrokendome.gradle.plugins.xjc.XjcSourceSetConvention;

import java.util.Arrays;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.ehcache.build.plugins.VariantPlugin.ProjectVersionedCapability.capabilityOf;
import static org.ehcache.build.util.ConfigurationBucketSet.bucketsFor;
import static org.ehcache.build.util.PluginUtils.camel;
import static org.ehcache.build.util.PluginUtils.capitalize;
import static org.ehcache.build.util.PluginUtils.lower;
import static org.ehcache.build.util.PluginUtils.registerBucket;
import static org.gradle.api.attributes.DocsType.SOURCES;
import static org.gradle.api.plugins.JavaPlugin.API_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.RUNTIME_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.plugins.JavaPlugin.SOURCES_ELEMENTS_CONFIGURATION_NAME;
import static org.gradle.api.tasks.SourceSet.MAIN_SOURCE_SET_NAME;

public class VariantPlugin implements Plugin<Project> {

  protected static final String COMMON_KIND = "common";

  @Override
  public void apply(Project project) {
    VariantExtension variants = project.getExtensions().create("variants", VariantExtension.class, project);
    configureJavaPluginBehavior(project, variants);
  }

  private void configureJavaPluginBehavior(Project project, VariantExtension variants) {
    project.getPlugins().withType(JavaPlugin.class).configureEach(javaPlugin ->  {
      JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);

      ConfigurationBucketSet commonBuckets = bucketsFor(project, COMMON_KIND);


      SourceSet mainSourceSet = java.getSourceSets().getByName(MAIN_SOURCE_SET_NAME);
      Variant defaultVariant = variants.getDefaultVariant();
      ConfigurationBucketSet defaultBuckets = bucketsFor(project, mainSourceSet).extendFrom(commonBuckets);
      defaultVariant.getConfigTraits().configureEach(trait ->
        defaultBuckets.extendFrom(bucketsFor(project, camel(trait, "trait")))
      );
      defaultVariant.getCapabilities().configureEach(capability -> {
        project.getConfigurations().named(mainSourceSet.getApiElementsConfigurationName()).configure(c -> c.getOutgoing().capability(capability));
        project.getConfigurations().named(mainSourceSet.getRuntimeElementsConfigurationName()).configure(c -> c.getOutgoing().capability(capability));
        project.getConfigurations().named(mainSourceSet.getSourcesElementsConfigurationName()).configure(c -> c.getOutgoing().capability(capability));
      });

      variants.getVariants().configureEach(variant -> {
        ConfigurationBucketSet variantBuckets;
        if (variant.hasPrivateSources().get()) {
          SourceSet commonSources = java.getSourceSets().findByName(COMMON_KIND);
          if (commonSources == null) {
            commonSources = java.getSourceSets().create(COMMON_KIND, common -> {
              project.getTasks().named(common.getCompileJavaTaskName(), task -> task.setEnabled(false));
              project.getTasks().named(common.getClassesTaskName(), task -> task.setEnabled(false));
              NamedDomainObjectProvider<Configuration> isolatedCompileOnly = registerBucket(project, "isolatedCompileOnly", COMMON_KIND);
              project.getConfigurations().named(common.getCompileClasspathConfigurationName()).configure(c -> {
                c.extendsFrom(isolatedCompileOnly.get());
              });
              linkToCommonSource(project, common, mainSourceSet);
            });
          }
          SourceSet variantSources = java.getSourceSets().create(variant.getName());

          java.registerFeature(variant.getName(), feature -> {
            feature.usingSourceSet(variantSources);
            feature.withSourcesJar();
            variant.getCapabilities().configureEach(capability -> feature.capability(capability.getGroup(), capability.getName(), requireNonNull(capability.getVersion())));
          });
          linkToCommonSource(project, commonSources, variantSources);

          project.getPlugins().withType(BndBuilderPlugin.class).configureEach(bnd -> {
            project.getTasks().named(variantSources.getJarTaskName(), Jar.class, jar -> {
              jar.setDescription("Assembles a bundle containing the " + variant + " variant classes.");
              BundleTaskExtension extension = jar.getExtensions().create(BundleTaskExtension.NAME, BundleTaskExtension.class, jar);
              BndConvention.configureBundleDefaults(project, extension);
              jar.doLast("buildBundle", extension.buildAction());
            });
          });

          project.getTasks().named(LifecycleBasePlugin.ASSEMBLE_TASK_NAME).configure(task -> {
            task.dependsOn(variantSources.getJarTaskName());
            task.dependsOn(variantSources.getSourcesJarTaskName());
          });

          variantBuckets = bucketsFor(project, variantSources);
        } else {
          SourceSet sharedSource = variant.getSharedSourceSet().get();

          JvmPluginServices jvmPluginServices = ((ProjectInternal) project).getServices().get(JvmPluginServices.class);

          variantBuckets = bucketsFor(project, variant.getName());

          Configuration apiElements = jvmPluginServices.createOutgoingElements(lower(camel(variant.getName(), API_ELEMENTS_CONFIGURATION_NAME)), builder ->
            builder.fromSourceSet(sharedSource).extendsFrom(variantBuckets.api()).extendsFrom(variantBuckets.compileOnlyApi()).withClassDirectoryVariant().providesApi());
          project.getConfigurations().named(sharedSource.getApiElementsConfigurationName(),
            config -> config.getOutgoing().getArtifacts().configureEach(artifact -> apiElements.getOutgoing().getArtifacts().add(artifact)));

          Configuration runtimeElements = jvmPluginServices.createOutgoingElements(lower(camel(variant.getName(), RUNTIME_ELEMENTS_CONFIGURATION_NAME)), builder ->
            builder.fromSourceSet(sharedSource).published().extendsFrom(variantBuckets.implementation()).extendsFrom(variantBuckets.runtimeOnly()).providesRuntime());
          project.getConfigurations().named(sharedSource.getRuntimeElementsConfigurationName(),
            config -> config.getOutgoing().getArtifacts().configureEach(artifact -> runtimeElements.getOutgoing().getArtifacts().add(artifact)));

          Configuration sourcesElements = jvmPluginServices.createOutgoingElements(lower(camel(variant.getName(), capitalize(SOURCES_ELEMENTS_CONFIGURATION_NAME))), builder ->
            builder.fromSourceSet(sharedSource).published().providesAttributes(attributes -> attributes.documentation(SOURCES).asJar()));
          project.getConfigurations().named(sharedSource.getSourcesElementsConfigurationName(),
            config -> config.getOutgoing().getArtifacts().configureEach(artifact -> sourcesElements.getOutgoing().getArtifacts().add(artifact)));

          variant.getCapabilities().configureEach(capability -> {
            apiElements.getOutgoing().capability(capability);
            runtimeElements.getOutgoing().capability(capability);
            sourcesElements.getOutgoing().capability(capability);
          });
        }

        variantBuckets.extendFrom(commonBuckets);
        variant.getConfigTraits().configureEach(trait ->
          variantBuckets.extendFrom(bucketsFor(project, camel(trait, "trait")))
        );
      });
    });
  }

  private static void linkToCommonSource(Project project, SourceSet commonSources, SourceSet derivedSources) {
    registerCommonCopyTask(project, commonSources, derivedSources, SourceSet::getJava);
    registerCommonCopyTask(project, commonSources, derivedSources, SourceSet::getResources);
    bucketsFor(project, derivedSources).extendFrom(bucketsFor(project, commonSources));

    project.getPlugins().withType(XjcPlugin.class).configureEach(plugin -> {
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
    Provider<Directory> variantLocation = project.getLayout().getBuildDirectory().dir("generated/sources/common/" + variant.getName() + "/" + commonSource.getName());
    TaskProvider<Sync> variantTask = project.getTasks().register(variant.getTaskName("copyCommon", commonSource.getName()), Sync.class, sync -> {
      sync.from(commonSource);
      sync.into(variantLocation);
    });
    type.apply(variant).srcDir(variantTask);
  }

  public static class Variant {

    private static final NotationParser<Object, Capability> CAPABILITY_NOTATION_PARSER = new CapabilityNotationParserFactory(false).create();

    private final Project project;
    private final String name;
    private final Property<Boolean> hasPrivateSources;
    private final Property<SourceSet> sharedSourceSet;
    private final DomainObjectSet<String> configTraits;
    private final DomainObjectSet<Capability> capabilities;

    public Variant(String name, Project project) {
      this.project = project;
      this.name = name;
      ObjectFactory objectFactory = project.getObjects();
      this.hasPrivateSources = objectFactory.property(Boolean.class).convention(false);
      this.sharedSourceSet = objectFactory.property(SourceSet.class).convention(project.provider(() ->
        project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().getByName(MAIN_SOURCE_SET_NAME)
      ));
      this.configTraits = objectFactory.domainObjectSet(String.class);
      this.capabilities = objectFactory.domainObjectSet(Capability.class);

      this.hasPrivateSources.finalizeValueOnRead();
      this.sharedSourceSet.finalizeValueOnRead();
    }

    public String getName() {
      return name;
    }

    protected Provider<SourceSet> getSharedSourceSet() {
      return sharedSourceSet;
    }

    protected Provider<Boolean> hasPrivateSources() {
      return hasPrivateSources;
    }

    protected DomainObjectSet<String> getConfigTraits() {
      return configTraits;
    }

    protected DomainObjectSet<Capability> getCapabilities() {
      return capabilities;
    }

    public void separateSource() {
      hasPrivateSources.set(true);
      sharedSourceSet.set((SourceSet) null);
    }

    public void sourcesFrom(SourceSet sourceSet) {
      hasPrivateSources.set(false);
      sharedSourceSet.set(sourceSet);
    }

    public void traits(String ... traits) {
      configTraits.addAll(Arrays.asList(traits));
    }

    public void capability(Object notation) {
      capabilities.add(capabilityOf(project,  CAPABILITY_NOTATION_PARSER.parseNotation(notation)));
    }
  }

  public static class VariantExtension {

    private final Project project;
    private final Variant defaultVariant;
    private final NamedDomainObjectContainer<Variant> variants;

    public VariantExtension(Project project) {
      this.project = project;
      this.defaultVariant = new Variant(null, project);
      this.variants = project.container(Variant.class);
    }

    public void defaultVariant(Action<Variant> action) {
      action.execute(defaultVariant);
    }

    public void variant(String variant, Action<Variant> action) {
      Variant v = new Variant(variant, project);
      action.execute(v);
      variants.add(v);
    }

    public Variant getDefaultVariant() {
      return defaultVariant;
    }

    public NamedDomainObjectContainer<Variant> getVariants() {
      return variants;
    }
  }

  static class ProjectVersionedCapability implements Capability {

    private final Project project;
    private final Capability capability;

    public static Capability capabilityOf(Project project, Capability capability) {
      if (capability.getVersion() == null) {
        return new ProjectVersionedCapability(project, capability);
      } else {
        return capability;
      }
    }

    private ProjectVersionedCapability(Project project, Capability capability) {
      this.project = project;
      this.capability = capability;
    }

    @Override
    public String getGroup() {
      return capability.getGroup();
    }

    @Override
    public String getName() {
      return capability.getName();
    }

    @Override
    public String getVersion() {
      return project.getVersion().toString();
    }
  }
}
