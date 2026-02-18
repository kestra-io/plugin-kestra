package io.kestra.plugin.ee.assets;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.AbstractKestraEeContainerTest;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.plugin.kestra.ee.assets.Set;
import io.kestra.sdk.model.AssetsControllerApiAsset;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class SetTest extends AbstractKestraEeContainerTest {
    protected static final String NAMESPACE = "kestra.tests.assets.set";

    @Inject
    TestRunContextFactory runContextFactory;

    @Test
    void defaultSet() throws Exception {
        // Given
        Set set = Set.builder()
            .id(io.kestra.plugin.core.kv.Set.class.getSimpleName())
            .type(io.kestra.plugin.core.kv.Set.class.getName())
            .kestraUrl(Property.ofValue(KESTRA_URL))
            .auth(AbstractKestraTask.Auth.builder()
                .username(Property.ofValue(USERNAME))
                .password(Property.ofValue(PASSWORD))
                .build()
            )
            .tenantId(Property.ofValue(TENANT_ID))
            .namespace(Property.ofExpression("{{ inputs.namespace }}"))
            .assetId(Property.ofExpression("{{ inputs.assetId }}"))
            .assetType(Property.ofExpression("{{ inputs.assetType }}"))
            .displayName(Property.ofExpression("{{ inputs.displayName }}"))
            .assetDescription(Property.ofExpression("{{ inputs.assetDescription }}"))
            .metadata(Property.ofExpression("{{ inputs.metadata }}"))
            .build();

        String assetId = IdUtils.create();
        String assetType = "MY_ASSET_TYPE";
        String displayName = "My Asset Name";
        String description = "My Asset Description";
        Map<String, String> metadata = Map.of(
            "system", "my-system",
            "database", "my-project",
            "schema", "analytics",
            "name", "customers_by_country"
        );
        final RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, set, Map.of(
            "namespace", NAMESPACE,
            "assetId", assetId,
            "assetType", assetType,
            "displayName", displayName,
            "assetDescription", description,
            "metadata", metadata
        ));

        // When
        set.run(runContext);

        AssetsControllerApiAsset asset = kestraTestDataUtils.getAsset(assetId);
        assertThat(asset.getId()).isEqualTo(assetId);
        assertThat(asset.getType()).isEqualTo(assetType);
        assertThat(asset.getDisplayName()).isEqualTo(displayName);
        assertThat(asset.getDescription()).isEqualTo(description);
        assertThat(asset.getMetadata()).isEqualTo(metadata);
    }
}
