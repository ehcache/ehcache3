import org.gradle.api.JavaVersion
import org.gradle.api.resources.MissingResourceException;
import org.gradle.internal.os.OperatingSystem;

/**
 * Emulates maven toolchains support by looking at the user's
 *    ~/.m2/toolchains.xml
 *
 * Throws if this file is not found
 *
 * Provides a closure to use to find the correct jvm's executable, eg:
 *  MavenToolchain.javaExecutable(JavaVersion.VERSION_1_8, 'javac')
 */
class MavenToolchain {

  static def mavenToolchainDefinitions = {
    String userHome = System.getProperty("user.home");
    File toolchain = new File(userHome, ".m2" + File.separator + "toolchains.xml")
    if (toolchain.isFile()) {
      def xmlSlurper = new XmlSlurper()
      return new XmlSlurper().parse(toolchain)
    } else {
      throw new MissingResourceException("toolchain file not found at ${toolchain}" );
    }
  }

  static def toolchains;
  static {
    def xml = mavenToolchainDefinitions()
    if (xml == null) {
      toolchains = [:]
    } else {
      toolchains = xml.toolchain.findAll({ it.type.text() == 'jdk' }).collectEntries{[JavaVersion.toVersion(it.provides.version.text()), it.configuration.jdkHome.text()]}
    }
  }

  private static def exe = OperatingSystem.current().isWindows() ? '.exe' : ''

  static def javaHome = { v -> 
    def jdk = toolchains.get(v);
    if (jdk == null) {

      throw new MissingResourceException("JDK $v not available - check your toolchains.xml")
    } else {
      return jdk;
    }
  }

  static def javaExecutable = { v, exec -> MavenToolchain.javaHome(v) + ['', 'bin', exec].join(File.separator) + exe }
}
