package io.kestra.plugin.kestra;

import java.util.Optional;
import java.util.function.Supplier;

import io.kestra.core.runners.SDK;

import jakarta.annotation.Nullable;

/**
 * The default SDK authentication, looked up at most once.
 *
 * <p>
 * On the Enterprise Edition the lookup reads the namespace and then the tenant metastore, so resolving the URL and
 * the credentials from separate calls would pay for it twice. An SDK that is absent, or that reports no default
 * authentication at all, is reported as {@link Optional#empty()} rather than propagated as a null.
 */
final class DefaultAuthSupplier implements Supplier<Optional<SDK.Auth>> {
    private final SDK sdk;
    private Optional<SDK.Auth> resolved;

    DefaultAuthSupplier(@Nullable SDK sdk) {
        this.sdk = sdk;
    }

    @Override
    public Optional<SDK.Auth> get() {
        if (resolved == null) {
            resolved = Optional.ofNullable(sdk).map(SDK::defaultAuthentication).orElse(Optional.empty());
        }
        return resolved;
    }
}
