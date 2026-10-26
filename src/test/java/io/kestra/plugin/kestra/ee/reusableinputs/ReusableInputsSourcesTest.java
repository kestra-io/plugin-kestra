package io.kestra.plugin.kestra.ee.reusableinputs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.sdk.internal.ApiException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReusableInputsSourcesTest {
    private static final String BOM = """
        id: bom
        namespace: company.team
        description: Bill of materials
        inputs:
          - id: environment
            type: SELECT
            values: [dev, prod]
            defaults: dev
          - id: sizing
            type: FORM
            inputs:
              - id: cpus
                type: INT
                min: 1
                defaults: 2
        """;

    private static final ReusableInputsSources.FileOpener NO_FILE = uri ->
    {
        throw new AssertionError("no file expected, got " + uri);
    };

    private static final String SERVERS = """
        id: bom
        namespace: company.team
        inputs:
          - id: servers
            type: TABLE
            columns:
              - id: name
                type: STRING
              - id: cpus
                type: INT
              - id: ratio
                type: FLOAT
              - id: managed
                type: BOOL
        """;

    private static Map<String, Object> parse(String source) throws Exception {
        return JacksonMapper.ofYaml().readValue(source, new TypeReference<>() {
        });
    }

    @Test
    void shouldOnlyChangeTheDefaultsOfExistingInputsWhenDefaultsAreSet() throws Exception {
        String source = ReusableInputsSources.apply(BOM, "company.team", "bom", null, null, Map.of("environment", "prod", "sizing.cpus", 4), NO_FILE);

        assertThat(parse(source)).isEqualTo(parse(BOM.replace("defaults: dev", "defaults: prod").replace("defaults: 2", "defaults: 4")));
    }

    @Test
    void shouldReplaceEveryInputAndKeepTheDescriptionWhenInputsAreSet() throws Exception {
        String source = ReusableInputsSources.apply(BOM, "company.team", "bom", null, List.of(Map.of("id", "region", "type", "STRING")), null, NO_FILE);

        assertThat(parse(source))
            .containsEntry("description", "Bill of materials")
            .containsEntry("inputs", List.of(Map.of("id", "region", "type", "STRING")));
    }

    @Test
    void shouldSetTheDefaultsOfInputsDeclaredInTheSameTaskWhenCreatingASet() throws Exception {
        String source = ReusableInputsSources.apply(
            null, "company.team", "bom", "New", List.of(Map.of("id", "region", "type", "STRING")), Map.of("region", "eu"), NO_FILE
        );

        assertThat(parse(source)).isEqualTo(
            Map.of(
                "id", "bom",
                "namespace", "company.team",
                "description", "New",
                "inputs", List.of(Map.of("id", "region", "type", "STRING", "defaults", "eu"))
            )
        );
    }

    @Test
    void shouldFailWhenADefaultTargetsAnInputTheSetDoesNotHave() {
        assertThatThrownBy(() -> ReusableInputsSources.apply(BOM, "company.team", "bom", null, null, Map.of("sizing.memory", 8), NO_FILE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("`sizing.memory` is not an input of reusable inputs 'bom'.");
    }

    @Test
    void shouldFailWhenCreatingASetWithoutInputs() {
        assertThatThrownBy(() -> ReusableInputsSources.apply(null, "company.team", "bom", null, null, Map.of("environment", "prod"), NO_FILE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("`inputs` must be set to create them");
    }

    @Test
    void shouldReportTheProblemDetailWhenTheApiRejectsTheSet() {
        ApiException rejected = new ApiException(
            422, "Unprocessable Entity", Map.of(), "{\"title\":\"Unprocessable Entity\",\"detail\":\"cpus: Invalid default: `abc` is not a valid INT value\"}"
        );

        assertThat(ReusableInputsSources.errorMessage(rejected)).isEqualTo("cpus: Invalid default: `abc` is not a valid INT value");
    }

    @Test
    void shouldReadTheRowsOfATableFromAFileTypingEachTextCellAfterItsColumnAndDroppingEmptyOnes() throws Exception {
        URI file = URI.create("kestra:///company/team/servers.ion");
        String rows = "{name:\"web\",cpus:\"2\",ratio:\"0.5\",managed:\"TRUE\"}\n{name:\"\",cpus:\"abc\",ratio:\"\",managed:\"maybe\"}\n";

        String source = ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", file.toString()), uri ->
        {
            assertThat(uri).isEqualTo(file);
            return stream(rows);
        });

        assertThat(inputsOf(parse(source)).getFirst().get("defaults")).isEqualTo(
            List.of(Map.of("name", "web", "cpus", 2, "ratio", 0.5, "managed", true), Map.of("cpus", "abc", "managed", "maybe"))
        );
    }

    @Test
    void shouldReadTheRowsOfATableFromACsvFileByItsHeader() throws Exception {
        String csv = "managed,name,cpus\ntrue,web,2\n,pg,4\n";

        String source = ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv));

        assertThat(inputsOf(parse(source)).getFirst().get("defaults")).isEqualTo(
            List.of(Map.of("managed", true, "name", "web", "cpus", 2), Map.of("name", "pg", "cpus", 4))
        );
    }

    @Test
    void shouldReadACsvFileExportedByAFrenchExcelWithItsByteOrderMarkAndSemicolons() throws Exception {
        String csv = "\uFEFFname;cpus\r\n\"web, front\";2\r\n";

        String source = ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv));

        assertThat(inputsOf(parse(source)).getFirst().get("defaults")).isEqualTo(List.of(Map.of("name", "web, front", "cpus", 2)));
    }

    @Test
    void shouldReadAMultiselectCellAsAListOfValues() throws Exception {
        String tags = """
            id: bom
            namespace: company.team
            inputs:
              - id: servers
                type: TABLE
                columns:
                  - id: name
                    type: STRING
                  - id: tags
                    type: MULTISELECT
                    values: [web, db]
            """;
        String csv = "name,tags\nfront,\"web, db\"\nback,\"[\"\"db\"\"]\"\n";

        String source = ReusableInputsSources.apply(tags, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv));

        assertThat(inputsOf(parse(source)).getFirst().get("defaults")).isEqualTo(
            List.of(Map.of("name", "front", "tags", List.of("web", "db")), Map.of("name", "back", "tags", List.of("db")))
        );
    }

    @Test
    void shouldReadACsvFileExcelWroteInWindows1252() throws Exception {
        byte[] csv = "name,cpus\nCafé,2\n".getBytes(Charset.forName("windows-1252"));

        String source = ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> new ByteArrayInputStream(csv));

        assertThat(inputsOf(parse(source)).getFirst().get("defaults")).isEqualTo(List.of(Map.of("name", "Café", "cpus", 2)));
    }

    @Test
    void shouldReadADecimalCommaAndIgnoreAnEmptyHeaderCell() throws Exception {
        String csv = "name;ratio;\nweb;0,5;\n";

        String source = ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv));

        assertThat(inputsOf(parse(source)).getFirst().get("defaults")).isEqualTo(List.of(Map.of("name", "web", "ratio", 0.5)));
    }

    @Test
    void shouldFailOnAnEmptyFileInsteadOfEmptyingTheTable() {
        assertThatThrownBy(() -> ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream("")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("The CSV file of `servers` in reusable inputs 'bom' can't be read: the file has no header line");
    }

    @Test
    void shouldFailOnAHeaderThatIsNotAColumnOfTheTable() {
        String csv = "name,memory\nweb,8\n";

        assertThatThrownBy(() -> ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("`memory` is not a column of `servers` in reusable inputs 'bom'. Its columns are: name, cpus, ratio, managed.");
    }

    @Test
    void shouldFailOnACsvRowWithMoreCellsThanTheHeaderInsteadOfDroppingTheExtraOnes() {
        String csv = "name,cpus\nweb, front,2\n";

        assertThatThrownBy(() -> ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("The CSV file of `servers` in reusable inputs 'bom' can't be read: ");
    }

    @Test
    void shouldFailOnACsvHeaderNamingAColumnTwice() {
        String csv = "name,cpus,name\nweb,2,api\n";

        assertThatThrownBy(() -> ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", "kestra:///servers.csv"), uri -> stream(csv)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("The CSV file of `servers` in reusable inputs 'bom' can't be read: `name` appears more than once in the header");
    }

    @Test
    void shouldFailOnAKeyThatIsNotAColumnOfTheTableInRowsWrittenInTheTask() {
        assertThatThrownBy(() -> ReusableInputsSources.apply(SERVERS, "company.team", "bom", null, null, Map.of("servers", List.of(Map.of("name", "web", "memory", 8))), NO_FILE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("`memory` is not a column of `servers`");
    }

    @Test
    void shouldKeepAFileUriAsItIsForAnInputThatIsNotATable() throws Exception {
        String bom = BOM.replace("type: SELECT\n    values: [dev, prod]", "type: STRING");

        String source = ReusableInputsSources.apply(bom, "company.team", "bom", null, null, Map.of("environment", "kestra:///env.txt"), NO_FILE);

        assertThat(inputsOf(parse(source)).getFirst()).containsEntry("defaults", "kestra:///env.txt");
    }

    @Test
    void shouldFindTheChildOfAFormWhoseIdContainsADot() throws Exception {
        String source = """
            id: bom
            namespace: company.team
            inputs:
              - id: app.sizing
                type: FORM
                inputs:
                  - id: cpu.count
                    type: INT
            """;

        String updated = ReusableInputsSources.apply(source, "company.team", "bom", null, null, Map.of("app.sizing.cpu.count", 4), NO_FILE);

        assertThat(inputsOf(inputsOf(parse(updated)).getFirst()).getFirst()).containsEntry("defaults", 4);
    }

    private static InputStream stream(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> inputsOf(Map<String, Object> set) {
        return (List<Map<String, Object>>) set.get("inputs");
    }
}
