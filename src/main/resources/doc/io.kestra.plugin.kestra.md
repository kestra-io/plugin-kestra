# How to use the Kestra plugin

Interact with a Kestra instance — manage executions, flows, logs, namespaces, and triggers — from within a Kestra flow.

## Authentication

Set `kestraUrl` to the target instance URL. Authenticate via `auth.apiToken` (Bearer token), or `auth.username` and `auth.password` (Basic auth). Set `auth.auto: true` (default) to automatically use the current instance's credentials when running inside Kestra itself. For multi-tenant instances, set `tenantId`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

### Executions

`executions.Count` counts executions matching optional filters — filter by `namespaces`, `flowId`, `states`, `startDate`, `endDate`, or `expression`. The output includes `count`.

`executions.Query` searches executions — filter by `namespace`, `flowId`, `states`, `labels`, `startDate`, `endDate`, `timeRange`, and more. Control paging with `page` and `size` (default 10). Set `fetchType` (default `STORE`) to control output. The output includes `size` and `uri`.

`executions.Delete` deletes a terminated execution — set `executionId` (required). Set `deleteLogs`, `deleteMetrics`, and `deleteStorage` to control what is cleaned up (all default `true`).

`executions.Kill` kills a running execution — set `executionId` (required). Set `propagateKill: false` to prevent cascading (default `true`).

`executions.Resume` resumes a paused execution — set `executionId`. Optionally pass `inputs`.

### Flows

`flows.List` lists flows in a namespace — optionally filter by `namespace`.

`flows.Export` exports flows as a ZIP archive — optionally filter by `namespace` and `labels`. The output includes `flowsZip` (URI).

`flows.ExportById` exports specific flows by `flows` (list of `{namespace, id}` objects). The output includes `flowsZip` (URI).

### Logs

`logs.Fetch` fetches execution logs to internal storage — optionally filter by `namespace`, `flowId`, `executionId`, `tasksId`, and `level` (default `INFO`). The output includes `size` and `uri`.

### Namespaces

`namespaces.List` lists namespaces — optionally filter by `prefix`, paginate with `page` and `size` (default 10). Set `existingOnly: true` to exclude empty namespaces (default `false`). The output includes `namespaces`.

`namespaces.NamespacesWithFlows` lists distinct namespaces that contain flows — optionally filter by `prefix`. The output includes `namespaces`.

### Triggers

`triggers.Toggle` enables or disables a trigger — set `flowId`, `namespace`, `trigger`, and `enabled` (default `false`).

## Triggers

`triggers.ScheduleMonitor` polls for unhealthy schedule triggers — filter by `namespace` and `flowId`. Set `allowedDelay` (default 1 minute) and optionally `maxExecutionDuration` or `maxExecutionInterval`. The polling `interval` defaults to 60 seconds.

## Enterprise Edition tasks

`ee/assets.Set` creates or updates an asset — set `assetId` and `assetType` (both required). Optionally set `namespace`, `displayName`, `assetDescription`, and `metadata`.

`ee/assets.List` lists assets — filter by `namespace`, `types`, and `metadataQuery`. Set `fetchType` (default `STORE`). The output includes `assets`, `asset`, `uri`, and `size`.

`ee/assets.Delete` deletes an asset by `assetId` (required).

`ee/assets.PurgeAssets` purges assets before an `endDate` (required) — control what is purged with `purgeAssets`, `purgeAssetUsages`, and `purgeAssetLineages` (all default `true`).

`ee/tests.RunTest` runs a single test suite — set `namespace` and `testId` (both required). Optionally set `testCases` and `failOnTestFailure` (default `false`).

`ee/tests.RunTests` runs all tests matching optional filters — set `namespace`, `flowId`, `includeChildNamespaces` (default `true`), and `failOnTestFailure` (default `false`).

`ee/assets.FreshnessTrigger` monitors asset freshness — set `maxStaleness` (required). Filter by `assetId`, `namespace`, `assetType`, and `metadataQuery`. The polling `interval` defaults to 1 hour.
