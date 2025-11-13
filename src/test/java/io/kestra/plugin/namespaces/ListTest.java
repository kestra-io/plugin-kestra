package io.kestra.plugin.namespaces;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.AbstractKestraContainerTest;
import io.kestra.plugin.AbstractKestraEeContainerTest;
import io.kestra.plugin.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.namespaces.List;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public class ListTest extends AbstractKestraEeContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.namespaces.list";

    @Test
    public void shouldListNamespaces() throws Exception {
        RunContext runContext = runContextFactory.of();

        String NAMESPACE_LOCAL = NAMESPACE + ".shouldlistnamespaces";

        List listNamespaces = listTask(NAMESPACE_LOCAL, null);

        kestraTestDataUtils.createRandomizedNamespace(NAMESPACE_LOCAL);
        kestraTestDataUtils.createRandomizedNamespace(NAMESPACE_LOCAL + ".sub");

        List.Output listFlowsOutput = listNamespaces.run(runContext);

        assertThat(listFlowsOutput.getNamespaces().size(), is(2));
    }

    @Test
    public void shouldListNamespacesWithPagination() throws Exception {
        RunContext runContext = runContextFactory.of();
        Integer NAMESPACE_COUNT = 20;
        String NAMESPACE_LOCAL = NAMESPACE + ".withpagination";

        List listNamespaces = listTask(NAMESPACE_LOCAL, null);

        for (int i = 0; i < 20; i++) {
            kestraTestDataUtils.createRandomizedFlow(NAMESPACE_LOCAL + ".namespace" + i);
        }
        List.Output listNamespacesOutput = listNamespaces.run(runContext);

        assertThat(listNamespacesOutput.getNamespaces().size(), is(NAMESPACE_COUNT));

        listNamespaces = listTask(NAMESPACE_LOCAL, 1);
        listNamespacesOutput = listNamespaces.run(runContext);

        assertThat(listNamespacesOutput.getNamespaces().size(), is(10));
    }

    /**
     * Required because using `toBuilder()` with Property does not work as expected
     */
    private List listTask(String prefix, @Nullable Integer page) {
        List.ListBuilder<?, ?> listBuilder = List.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .prefix(Property.ofValue(prefix));

        if (page != null) {
            listBuilder.page(Property.ofValue(page));
        }

        return listBuilder.build();
    }
}
