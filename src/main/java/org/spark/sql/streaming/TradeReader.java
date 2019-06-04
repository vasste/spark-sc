package org.spark.sql.streaming;

import com.dxfeed.api.DXFeed;
import com.dxfeed.api.DXFeedSubscription;
import com.dxfeed.event.market.Trade;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author Vasilii Stepanov.
 * @since 19.11.2018
 */
public class TradeReader implements MicroBatchReader {

    private final DXFeedSubscription<Trade> subscription;
    private final DataSourceOptions options;
    private final StructType structType;

    private Offset start;
    private Offset end;

    public TradeReader(DataSourceOptions options) {
        this.options = options;
        this.structType = new StructType()
            .add("symbol", DataTypes.StringType)
            .add("price", DataTypes.DoubleType)
            .add("time", DataTypes.LongType);
        subscription = DXFeed.getInstance().createSubscription(Trade.class);
        options.get("symbol").ifPresent(subscription::addSymbols);
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        long time = System.currentTimeMillis();
        this.start = start.orElse(LongOffset.apply(time));
        this.end = end.orElse(LongOffset.apply(time + TimeUnit.SECONDS.toMillis(1)));
    }

    @Override
    public Offset getEndOffset() {
        return start;
    }

    @Override
    public Offset deserializeOffset(String json) {
        return null;
    }

    @Override
    public Offset getStartOffset() {
        return end;
    }

    @Override
    public void commit(Offset end) {

    }

    @Override
    public void stop() {
        subscription.close();
    }

    @Override
    public StructType readSchema() {
        return structType;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        return Collections.singletonList(new TradeInputPartition(subscription));
    }
}
