plugins {
    kotlin("jvm") version "2.0.0"
}

group = "org.alexiscrack3"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.beam:beam-sdks-java-core:2.45.0")
    implementation("org.apache.beam:beam-runners-direct-java:2.45.0")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}