plugins {
    java
    id("org.springframework.boot") version "3.3.2"
    id("io.spring.dependency-management") version "1.1.6"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.0.0"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()

    maven {
        url = uri("https://packages.confluent.io/maven")
    }

}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.apache.kafka:kafka-streams:3.5.1")
    implementation("io.confluent:kafka-streams-avro-serde:7.7.0")
    implementation("org.apache.avro:avro:1.11.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.awaitility:awaitility:4.2.1")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
