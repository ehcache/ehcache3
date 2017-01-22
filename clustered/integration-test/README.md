How to launch tests from an IDE
-------------------------------

1. Create the kit with `gradle :clustered:integration-test:copyServerLibs`
2. Launch the test once to make it fail
3. Edit the test configuration to add the following VM options:

`-Dcom.tc.l2.lockmanager.greedy.locks.enabled=false -DkitInstallationPath=build/ehcache-kit/ehcache-clustered-3.3.0-SNAPSHOT-kit`
