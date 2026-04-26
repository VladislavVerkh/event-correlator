import org.gradle.api.publish.maven.MavenPublication

plugins {
    `java-library`
    id("io.spring.dependency-management")
    id("maven-publish")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    withSourcesJar()
    withJavadocJar()
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:4.0.3")
    }
}

dependencies {
    api(project(":event-correlator-core"))
    implementation(project(":event-correlator-postgres"))

    implementation("org.springframework.boot:spring-boot-autoconfigure")
    implementation("org.springframework:spring-jdbc")
    implementation("org.springframework:spring-tx")
    implementation("com.fasterxml.jackson.core:jackson-databind")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

publishing {
    publications {
        create("mavenJava", MavenPublication::class) {
            from(components["java"])
            artifactId = "event-correlator-spring-boot-starter"
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
