package io.kestra.plugin.kestra;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import lombok.extern.slf4j.Slf4j;

@Testcontainers
@Slf4j
public abstract class AbstractKestraOssContainerTest extends AbstractKestraContainerTest {
    @Container
    protected static final GenericContainer<?> KESTRA_OSS_CONTAINER = new AbstractKestraOssContainerTest() {
    }.buildContainer("kestra/kestra:v1.3-no-plugins", false);// TODO temporary solution, because develop has a regression somewhere

    @Override
    protected GenericContainer<?> getContainer() {
        return KESTRA_OSS_CONTAINER;
    }
}
