package io.kestra.plugin.kestra.ee.assets;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kestra.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.ee.assets.FieldQuery;
import io.kestra.plugin.kestra.ee.assets.List;
import io.kestra.plugin.kestra.ee.assets.QueryType;
import io.kestra.sdk.model.AssetsControllerApiAsset;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class ListTest extends AbstractKestraEeContainerTest {

    @Inject
    protected RunContextFactory runContextFactory;

    protected static final String NAMESPACE = "kestra.tests.assets.list";

    @Test
    void defaultList() throws Exception {
        String NAMESPACE_LOCAL = NAMESPACE + ".shouldlistassets";
        String ANOTHER_NAMESPACE_LOCAL = NAMESPACE + ".another.shouldlistassets";

        List listAssets = List.builder()
            .id("list_assets")
            .type(List.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofExpression("{{ inputs.namespace }}"))
            .types(Property.ofExpression("{{ inputs.assetTypes }}"))
            .metadataQuery(Property.ofExpression("{{ inputs.metadataQuery }}"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();
        final RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, listAssets, Map.of(
            "namespace", NAMESPACE_LOCAL,
            "assetTypes", java.util.List.of("MY_ASSET_TYPE", "ANOTHER_MATCHING_ASSET_TYPE"),
            "metadataQuery", java.util.List.of(
                new FieldQuery("myKey", QueryType.NOT_EQUAL_TO, "wrongValue"),
                new FieldQuery("anotherKey", QueryType.EQUAL_TO, "anotherValue")
            )
        ));

        Map<String, String> matchingMetadata = Map.of("myKey", "myValue", "anotherKey", "anotherValue");
        String matchingAsset1 = IdUtils.create();
        String matchingAsset2 = IdUtils.create();
        kestraTestDataUtils.createAsset(NAMESPACE_LOCAL, matchingAsset1, "MY_ASSET_TYPE", matchingMetadata);
        kestraTestDataUtils.createAsset(NAMESPACE_LOCAL, matchingAsset2, "ANOTHER_MATCHING_ASSET_TYPE", matchingMetadata);
        // Bad asset type
        kestraTestDataUtils.createAsset(NAMESPACE_LOCAL, IdUtils.create(), "ANOTHER_ASSET_TYPE", matchingMetadata);
        // Bad namespace
        kestraTestDataUtils.createAsset(ANOTHER_NAMESPACE_LOCAL, IdUtils.create(), "MY_ASSET_TYPE", matchingMetadata);
        // Bad metadata
        Map<String, String> wrongValueMetadata = new HashMap<>(matchingMetadata);
        wrongValueMetadata.put("myKey", "wrongValue");
        kestraTestDataUtils.createAsset(NAMESPACE_LOCAL, IdUtils.create(), "MY_ASSET_TYPE", wrongValueMetadata);

        // Default pagination, will return all matching assets and potentially iterate on multiple calls to retrieve the full list
        List.Output listAssetsOutput = listAssets.run(runContext);
        assertThat(listAssetsOutput.getAssets().stream().map(AssetsControllerApiAsset::getId).toList(), containsInAnyOrder(matchingAsset1, matchingAsset2));

        // There is no page 2 as the default page size is big enough to include all assets, so it should return an empty list
        listAssetsOutput = listAssets.toBuilder().page(Property.ofValue(2)).build().run(runContext);
        assertThat(listAssetsOutput.getAssets(), empty());

        // If page is not specified, it should return the full list but do multiple calls to get it if size < number of assets
        listAssetsOutput = listAssets.toBuilder().size(Property.ofValue(1)).build().run(runContext);
        assertThat(listAssetsOutput.getAssets().stream().map(AssetsControllerApiAsset::getId).toList(), containsInAnyOrder(matchingAsset1, matchingAsset2));

        // If page is specified, it should return only the assets of that page, as size is one and there are two matching assets, both page 1 and page 2 should return one of the two matching assets
        listAssetsOutput = listAssets.toBuilder().page(Property.ofValue(1)).size(Property.ofValue(1)).build().run(runContext);
        assertThat(listAssetsOutput.getAssets().stream().map(AssetsControllerApiAsset::getId).toList(), containsInAnyOrder(anyOf(
            is(matchingAsset1),
            is(matchingAsset2)
        )));
        listAssetsOutput = listAssets.toBuilder().page(Property.ofValue(2)).size(Property.ofValue(1)).build().run(runContext);
        assertThat(listAssetsOutput.getAssets().stream().map(AssetsControllerApiAsset::getId).toList(), containsInAnyOrder(anyOf(
            is(matchingAsset1),
            is(matchingAsset2)
        )));
    }

    @Test
    void fetchType() throws Exception {
        String NAMESPACE_LOCAL = NAMESPACE + ".fetchtype";

        String assetId1 = IdUtils.create();
        kestraTestDataUtils.createAsset(NAMESPACE_LOCAL, assetId1, "MY_ASSET_TYPE", Collections.emptyMap());
        String assetId2 = IdUtils.create();
        kestraTestDataUtils.createAsset(NAMESPACE_LOCAL, assetId2, "MY_ASSET_TYPE", Collections.emptyMap());

        List listAssets = List.builder()
            .id("list_assets")
            .type(List.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofValue(NAMESPACE_LOCAL))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, listAssets, null);
        // Default to store
        assertThat(runContext.render(listAssets.getFetchType()).as(FetchType.class).orElseThrow(), is(FetchType.STORE));

        runContext = TestsUtils.mockRunContext(this.runContextFactory, listAssets, null);
        List.Output run = listAssets.toBuilder().fetchType(Property.ofValue(FetchType.FETCH)).build().run(runContext);
        assertThat(run.getSize(), is(2L));
        assertThat(run.getAsset(), nullValue());
        assertThat(run.getUri(), nullValue());
        assertThat(run.getAssets().stream().map(AssetsControllerApiAsset::getId).toList(), containsInAnyOrder(assetId1, assetId2));

        runContext = TestsUtils.mockRunContext(this.runContextFactory, listAssets, null);
        run = listAssets.toBuilder().fetchType(Property.ofValue(FetchType.FETCH_ONE)).build().run(runContext);
        assertThat(run.getSize(), is(1L));
        assertThat(run.getAsset().getId(), oneOf(assetId1, assetId2));
        assertThat(run.getUri(), nullValue());
        assertThat(run.getAssets(), nullValue());

        runContext = TestsUtils.mockRunContext(this.runContextFactory, listAssets, null);
        run = listAssets.toBuilder().fetchType(Property.ofValue(FetchType.STORE)).build().run(runContext);
        assertThat(run.getSize(), is(2L));
        assertThat(run.getAsset(), nullValue());
        assertThat(run.getUri(), notNullValue());
        java.util.List<String> inFile = FileSerde.readAll(new InputStreamReader(runContext.storage().getFile(run.getUri())), AssetsControllerApiAsset.class)
            .map(AssetsControllerApiAsset::getId)
            .collectList().block();
        assertThat(inFile, containsInAnyOrder(assetId1, assetId2));
        assertThat(run.getAssets(), nullValue());

        runContext = TestsUtils.mockRunContext(this.runContextFactory, listAssets, null);
        run = listAssets.toBuilder().fetchType(Property.ofValue(FetchType.NONE)).build().run(runContext);
        assertThat(run.getSize(), nullValue());
        assertThat(run.getAsset(), nullValue());
        assertThat(run.getUri(), nullValue());
        assertThat(run.getAssets(), nullValue());


    }
}
