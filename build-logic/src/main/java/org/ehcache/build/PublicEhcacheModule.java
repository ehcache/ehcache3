package org.ehcache.build;

import org.gradle.api.Project;

public class PublicEhcacheModule extends EhcacheModule {
  @Override
  public void apply(Project project) {
    project.setGroup("org.ehcache");
    super.apply(project);
  }
}
