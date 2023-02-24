package org.ehcache.build;

import org.gradle.api.Project;
import org.gradle.api.tasks.bundling.Jar;

public class InternalEhcacheModule extends EhcacheModule {

  @Override
  public void apply(Project project) {
    project.setGroup("org.ehcache.modules");
    super.apply(project);

    project.getTasks().withType(Jar.class).configureEach(jar -> {
      jar.manifest(manifest -> {
        manifest.getAttributes().put("Automatic-Module-Name", "org.ehcache." + project.getName()
          .replaceAll("^ehcache-", "").replace("-", ".").replace("107", "jcache"));
      });
    });
  }
}

