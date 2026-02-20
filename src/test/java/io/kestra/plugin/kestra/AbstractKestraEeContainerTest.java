package io.kestra.plugin.kestra;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Slf4j
public abstract class AbstractKestraEeContainerTest extends AbstractKestraContainerTest {
    @Container
    protected static final GenericContainer<?> KESTRA_EE_CONTAINER = new AbstractKestraEeContainerTest(){}.buildContainer("ghcr.io/kestra-io/kestra-ee:develop", true);

    @Override
    protected GenericContainer<?> getContainer() {
        return KESTRA_EE_CONTAINER;
    }
}
