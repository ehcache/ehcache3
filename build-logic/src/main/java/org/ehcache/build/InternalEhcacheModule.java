package org.ehcache.build;

import org.gradle.api.Project;

public class InternalEhcacheModule extends EhcacheModule {

  @Override
  public void apply(Project project) {
    project.setGroup("org.ehcache.modules");
    super.apply(project);
  }
}

