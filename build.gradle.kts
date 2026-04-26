import com.diffplug.gradle.spotless.SpotlessExtension
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
    id("org.springframework.boot") version "4.0.3" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
    id("com.diffplug.spotless") version "7.2.1" apply false
    id("com.github.spotbugs") version "6.5.1" apply false
}

group = "dev.verkhovskiy"
version = "0.0.1-SNAPSHOT"

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    group = rootProject.group
    version = rootProject.version

    plugins.withType<JavaPlugin> {
        apply(plugin = "com.diffplug.spotless")
        apply(plugin = "com.github.spotbugs")
        apply(plugin = "jacoco")

        dependencies.add("compileOnly", "com.github.spotbugs:spotbugs-annotations:4.9.8")

        extensions.configure<SpotlessExtension> {
            java {
                target("src/*/java/**/*.java")
                googleJavaFormat("1.27.0")
            }
        }

        tasks.withType<Javadoc>().configureEach {
            (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:all,-missing", true)
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            jvmArgs("-Xshare:off")
            finalizedBy("jacocoTestReport")
        }

        tasks.named<JacocoReport>("jacocoTestReport") {
            dependsOn(tasks.withType<Test>())
            reports {
                xml.required.set(true)
                html.required.set(true)
            }
        }

        tasks.named("check") {
            dependsOn("spotlessCheck", "jacocoTestReport")
        }
    }
}

