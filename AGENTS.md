# Kestra Kestra Plugin

## What

Plugin for Kestra Exposes 20 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Kestra Plugin, allowing orchestration of Kestra Plugin-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
