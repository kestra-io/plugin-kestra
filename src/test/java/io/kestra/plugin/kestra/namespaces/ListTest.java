package io.kestra.plugin.kestra.namespaces;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kestra.AbstractKestraOssContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;

import jakarta.annotation.Nullable;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@KestraTest
public class ListTest extends AbstractKestraOssContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    public void shouldFilterNamespacesByPrefix() throws Exception {
        RunContext runContext = runContextFactory.of();
        String prefix = "kestra.tests.namespaces.list." + IdUtils.create().toLowerCase();
        String matching = prefix + ".matching";
        String nonMatching = "kestra.tests.namespaces.list.other." + IdUtils.create().toLowerCase();

        kestraTestDataUtils.createRandomizedFlow(matching);
        kestraTestDataUtils.createRandomizedFlow(nonMatching);

        List.Output output = listTask(prefix, null, null).run(runContext);

        assertThat(output.getNamespaces(), hasItem(matching));
        assertThat(output.getNamespaces(), not(hasItem(nonMatching)));
    }

    @Test
    public void shouldListNamespacesWithPagination() throws Exception {
        RunContext runContext = runContextFactory.of();
        // 20 generated namespaces + the queried prefix itself, as it's a parent namespace of the generated ones
        int namespaceCount = 21;
        String prefix = "kestra.tests.namespaces.list.pagination." + IdUtils.create().toLowerCase();

        for (int i = 0; i < 20; i++) {
            kestraTestDataUtils.createRandomizedFlow(prefix + ".namespace" + i);
        }

        List.Output allPages = listTask(prefix, null, null).run(runContext);
        assertThat(allPages.getNamespaces().size(), is(namespaceCount));

        List.Output firstPage = listTask(prefix, 1, null).run(runContext);
        assertThat(firstPage.getNamespaces().size(), is(10));
    }

    /**
     * Required because using `toBuilder()` with Property does not work as expected
     */
    private List listTask(String prefix, @Nullable Integer page, @Nullable Integer size) {
        List.ListBuilder<?, ?> listBuilder = List.builder()
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(
                AbstractKestraTask.Auth.builder()
                    .username(Property.ofValue(USERNAME))
                    .password(Property.ofValue(PASSWORD))
                    .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .prefix(Property.ofValue(prefix));

        if (page != null) {
            listBuilder.page(Property.ofValue(page));
        }

        if (size != null) {
            listBuilder.size(Property.ofValue(size));
        }

        return listBuilder.build();
    }
}
