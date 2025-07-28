package io.kestra.plugin.flows;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.namespaces.NamespacesWithFlows;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class NamespacesWithFlowsTest extends AbstractKestraContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.namespaces.distinctnamespaces";

    @Test
    public void shouldListNamespaces() throws Exception {
        RunContext runContext = runContextFactory.of();
        String subNamespace = NAMESPACE + ".sub";

        NamespacesWithFlows listNamespaces = distinctNamespacesTask(NAMESPACE);

        kestraTestDataUtils.createRandomizedFlow(NAMESPACE);
        kestraTestDataUtils.createRandomizedFlow(subNamespace);

        NamespacesWithFlows.Output listNamespacesOutput = listNamespaces.run(runContext);

        assertThat(listNamespacesOutput.getNamespaces().size(), is(2));

        listNamespacesOutput = distinctNamespacesTask(subNamespace).run(runContext);

        assertThat(listNamespacesOutput.getNamespaces().size(), is(1));
    }

    /**
     * Required because using `toBuilder()` with Property does not work as expected
     */
    private NamespacesWithFlows distinctNamespacesTask(String prefix) {
        return NamespacesWithFlows.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .prefix(Property.ofValue(prefix))
            .build();
    }
}
