package org.spark.sql.streaming;

import com.dxfeed.api.DXFeedSubscription;
import com.dxfeed.event.market.Trade;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

/**
 * @author Vasilii Stepanov.
 * @since 20.11.2018
 */
public class TradeInputPartition implements InputPartition<InternalRow> {

    private final DXFeedSubscription<Trade> subscription;

    public TradeInputPartition(DXFeedSubscription<Trade> subscription) {
        this.subscription = subscription;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new TradeInputPartitionReader();
    }
}
