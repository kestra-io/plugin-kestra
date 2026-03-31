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
        // the issue is here, some test work in 1.3 but not develop
    }.buildContainer("kestra/kestra:develop-no-plugins", false);

    @Override
    protected GenericContainer<?> getContainer() {
        return KESTRA_OSS_CONTAINER;
    }
}
