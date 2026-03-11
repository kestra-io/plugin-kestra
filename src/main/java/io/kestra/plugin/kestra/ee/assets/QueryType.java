package io.kestra.plugin.kestra.ee.assets;

import io.kestra.sdk.model.QueryFilterOp;

public enum QueryType {
    EQUAL_TO,
    NOT_EQUAL_TO;

    public QueryFilterOp toQueryFilterOp() {
        return switch (this) {
            case EQUAL_TO -> QueryFilterOp.EQUALS;
            case NOT_EQUAL_TO -> QueryFilterOp.NOT_EQUALS;
        };
    }
}
