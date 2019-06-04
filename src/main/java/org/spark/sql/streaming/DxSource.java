package org.spark.sql.streaming;

import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class DxSource implements DataSourceRegister, MicroBatchReadSupport {
    @Override
    public String shortName() {
        return "qds";
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
        return new TradeReader(options);
    }
}
