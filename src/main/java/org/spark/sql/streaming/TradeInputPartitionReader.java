package org.spark.sql.streaming;

import com.dxfeed.api.DXFeed;
import com.dxfeed.api.DXFeedEventListener;
import com.dxfeed.api.DXFeedSubscription;
import com.dxfeed.event.market.Trade;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Vasilii Stepanov.
 * @since 20.11.2018
 */
public class TradeInputPartitionReader implements InputPartitionReader<InternalRow> {

    @Override
    public boolean next() throws IOException {
        return true;
    }

    @Override
    public InternalRow get() {
        Trade trade = DXFeed.getInstance().getLastEvent(new Trade("AAPL"));
        return new GenericInternalRow(new Object[]{UTF8String.fromString(trade.getEventSymbol()), trade.getPrice(), trade.getTime()});
    }

    @Override
    public void close() throws IOException {}
}
