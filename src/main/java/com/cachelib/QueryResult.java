package com.cachelib;

import java.io.Serializable;
import java.util.List;

public final class QueryResult implements Serializable {
    private final int columnCount;
    private final List rows;

    public QueryResult(final int columnCount, final List rows) {
        this.columnCount = columnCount;
        this.rows = rows;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public List getRows() {
        return rows;
    }
}