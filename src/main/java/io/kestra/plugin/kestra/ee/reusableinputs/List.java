package io.kestra.plugin.kestra.ee.reusableinputs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.kestra.AbstractKestraTask;
import io.kestra.sdk.api.ReusableInputsApi;
import io.kestra.sdk.model.PagedResultsReusableInputsWithSource;
import io.kestra.sdk.model.ReusableInputsWithSource;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List reusable inputs",
    description = """
        Lists the sets of reusable inputs visible from a namespace, including the ones inherited from its parent namespaces, \
        with their id, namespace, description and revision. Each set is listed as the revision executions use, \
        and a set that only has drafts is left out, unless `allowDraft` is set. Fetches every page unless `page` is set.
        Enterprise Edition only."""
)
@Plugin(
    examples = {
        @Example(
            title = "List the reusable inputs of a namespace.",
            full = true,
            code = """
                id: reusable_inputs_list
                namespace: company.team

                tasks:
                  - id: list_reusable_inputs
                    type: io.kestra.plugin.kestra.ee.reusableinputs.List
                    fetchType: FETCH
                """
        )
    }
)
public class List extends AbstractKestraTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Namespace to list the reusable inputs of",
        description = "Defaults to the namespace of the flow."
    )
    @PluginProperty(group = "main")
    private Property<String> namespace;

    @Nullable
    @Schema(
        title = "Page number",
        description = "If omitted, all pages are fetched. Set to 1 or higher to retrieve a single page."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> page;

    @Nullable
    @Builder.Default
    @Schema(
        title = "Page size",
        description = "Number of sets per page; defaults to 100."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> size = Property.ofValue(100);

    @Schema(
        title = "Include draft revisions",
        description = "Each set is then listed as its latest revision, which may be a draft."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> allowDraft = Property.ofValue(false);

    @Schema(
        title = "Output fetch type",
        description = "Defines how results are returned: `FETCH` for direct output, `STORE` to persist as an ION file."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElseThrow();
        var rPage = runContext.render(this.page).as(Integer.class).orElse(null);
        var rSize = runContext.render(this.size).as(Integer.class).orElse(100);
        var rTenant = runContext.render(tenantId).as(String.class).orElse(runContext.flowInfo().tenantId());
        var rNamespace = runContext.render(namespace).as(String.class).orElseGet(() -> runContext.flowInfo().namespace());
        var rAllowDraft = runContext.render(this.allowDraft).as(Boolean.class).orElse(false);

        ReusableInputsApi api = kestraClient(runContext).reusableInputs();

        java.util.List<ReusableInputsWithSource> fetched;
        if (rPage != null) {
            fetched = api.listReusableInputs(rNamespace, rTenant, rPage, rSize, rAllowDraft).getResults();
        } else {
            fetched = new ArrayList<>();
            int currentPage = 1;
            long total;
            do {
                PagedResultsReusableInputsWithSource results = api.listReusableInputs(rNamespace, rTenant, currentPage, rSize, rAllowDraft);
                fetched.addAll(results.getResults());
                total = results.getTotal();
            } while ((long) currentPage++ * rSize < total);
        }
        java.util.List<Summary> summaries = fetched.stream().map(Summary::of).toList();

        var outputBuilder = Output.builder();
        switch (rFetchType) {
            case FETCH_ONE -> outputBuilder.reusableInputs(summaries.isEmpty() ? java.util.List.of() : java.util.List.of(summaries.getFirst()))
                .size(summaries.isEmpty() ? 0L : 1L);
            case STORE -> {
                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var fileWriter = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {
                    summaries.forEach(throwConsumer(summary ->
                    {
                        fileWriter.write(JacksonMapper.ofIon().writeValueAsString(summary));
                        fileWriter.write("\n");
                    }));
                }
                outputBuilder.uri(runContext.storage().putFile(tempFile)).size((long) summaries.size());
            }
            case FETCH -> outputBuilder.reusableInputs(summaries).size((long) summaries.size());
            case NONE -> runContext.logger().info("fetchType is NONE, no output returned");
        }

        return outputBuilder.build();
    }

    @Schema(title = "A set of reusable inputs visible from the namespace")
    public record Summary(
        @Schema(title = "ID of the reusable inputs") String id,
        @Schema(title = "Namespace the set is defined in, a parent namespace when it is inherited") String namespace,
        @Schema(title = "Description of the reusable inputs") String description,
        @Schema(title = "Revision listed: the one executions use, or the latest with `allowDraft`") Integer revision,
        @Schema(title = "Whether the revision listed is a draft") boolean draft) {
        static Summary of(ReusableInputsWithSource set) {
            return new Summary(set.getId(), set.getNamespace(), set.getDescription(), set.getRevision(), Boolean.TRUE.equals(set.getDraft()));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Fetched reusable inputs",
            description = "Only populated when using `fetchType=FETCH` or `fetchType=FETCH_ONE`."
        )
        private java.util.List<Summary> reusableInputs;

        @Schema(
            title = "Kestra internal storage URI of the stored reusable inputs",
            description = "Only populated when using `fetchType=STORE`."
        )
        private URI uri;

        @Schema(
            title = "Number of fetched reusable inputs",
            description = "Only populated if fetchType is not NONE."
        )
        private Long size;
    }
}
