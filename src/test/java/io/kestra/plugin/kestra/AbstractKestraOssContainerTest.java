package io.kestra.plugin.kestra;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Slf4j
public abstract class AbstractKestraOssContainerTest extends AbstractKestraContainerTest {
    @Container
    protected static final GenericContainer<?> KESTRA_OSS_CONTAINER =
        new AbstractKestraOssContainerTest(){}.buildContainer("kestra/kestra:develop-no-plugins", false);

    @Override
    protected GenericContainer<?> getContainer() {
        return KESTRA_OSS_CONTAINER;
    }
}
