# Kestra Kestra Plugin

## What

- Provides plugin components under `io.kestra.plugin.kestra`.
- Includes classes such as `List`, `NamespacesWithFlows`, `Fetch`, `List`.

## Why

- What user problem does this solve? Teams need to interact with Kestra itself to manage flows, executions, namespaces, and tests from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Plugin steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Plugin.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `kestra`

### Key Plugin Classes

- `io.kestra.plugin.kestra.ee.assets.Delete`
- `io.kestra.plugin.kestra.ee.assets.FreshnessTrigger`
- `io.kestra.plugin.kestra.ee.assets.List`
- `io.kestra.plugin.kestra.ee.assets.PurgeAssets`
- `io.kestra.plugin.kestra.ee.assets.Set`
- `io.kestra.plugin.kestra.ee.tests.RunTest`
- `io.kestra.plugin.kestra.ee.tests.RunTests`
- `io.kestra.plugin.kestra.executions.Count`
- `io.kestra.plugin.kestra.executions.Delete`
- `io.kestra.plugin.kestra.executions.Kill`
- `io.kestra.plugin.kestra.executions.Query`
- `io.kestra.plugin.kestra.executions.Resume`
- `io.kestra.plugin.kestra.flows.Export`
- `io.kestra.plugin.kestra.flows.ExportById`
- `io.kestra.plugin.kestra.flows.List`
- `io.kestra.plugin.kestra.logs.Fetch`
- `io.kestra.plugin.kestra.namespaces.List`
- `io.kestra.plugin.kestra.namespaces.NamespacesWithFlows`
- `io.kestra.plugin.kestra.triggers.ScheduleMonitor`
- `io.kestra.plugin.kestra.triggers.Toggle`

### Project Structure

```
plugin-kestra/
├── src/main/java/io/kestra/plugin/kestra/triggers/
├── src/test/java/io/kestra/plugin/kestra/triggers/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
