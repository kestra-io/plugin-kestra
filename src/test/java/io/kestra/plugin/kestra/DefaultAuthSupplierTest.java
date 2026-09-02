package io.kestra.plugin.kestra;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.kestra.core.runners.SDK;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DefaultAuthSupplierTest {
    private static final SDK.Auth AUTH = new SDK.Auth(Optional.of("https://example.com"), Optional.of("token"), Optional.empty(), Optional.empty());

    /** The URL and the credentials are resolved separately, and on the Enterprise Edition each lookup reads two metastores. */
    @Test
    void shouldLookTheDefaultAuthenticationUpOnlyOnce() {
        AtomicInteger lookups = new AtomicInteger();
        SDK sdk = () ->
        {
            lookups.incrementAndGet();
            return Optional.of(AUTH);
        };

        var supplier = new DefaultAuthSupplier(sdk);

        assertThat(supplier.get(), is(Optional.of(AUTH)));
        assertThat(supplier.get(), is(Optional.of(AUTH)));
        assertThat(lookups.get(), is(1));
    }

    @Test
    void shouldBeEmptyWhenThereIsNoSdk() {
        assertThat(new DefaultAuthSupplier(null).get(), is(Optional.empty()));
    }

    /** An SDK returning a bare null used to be memoized as "not computed yet", so it both leaked out and defeated the memoization. */
    @Test
    void shouldBeEmptyWhenTheSdkReportsNoAuthentication() {
        AtomicInteger lookups = new AtomicInteger();
        SDK sdk = () ->
        {
            lookups.incrementAndGet();
            return null;
        };

        var supplier = new DefaultAuthSupplier(sdk);

        assertThat(supplier.get(), is(Optional.empty()));
        assertThat(supplier.get(), is(Optional.empty()));
        assertThat(lookups.get(), is(1));
    }
}
