package org.feed;

import com.devexperts.qd.*;
import com.devexperts.qd.kit.PatternFilter;
import com.devexperts.qd.ng.RecordBuffer;
import com.devexperts.qd.ng.RecordCursor;
import com.devexperts.qd.ng.RecordMode;
import com.devexperts.qd.qtp.AgentAdapter;
import com.devexperts.qd.qtp.MessageConnector;
import com.devexperts.qd.qtp.MessageConnectors;
import com.devexperts.qd.stats.QDStats;
import com.dxfeed.api.impl.DXFeedScheme;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TradesFeed {

    private final QDTicker ticker = QDFactory.getDefaultFactory().tickerBuilder().withScheme(DXFeedScheme.getInstance()).build();
    private final QDStream stream = QDFactory.getDefaultFactory().streamBuilder().withScheme(DXFeedScheme.getInstance()).build();
    private final Set<String> subscribed = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public TradesFeed(String port) throws InterruptedException {
        AgentAdapter.Factory adapter = new AgentAdapter.Factory(ticker, stream, null,
                PatternFilter.valueOf("Trade", "tradeFilter", DXFeedScheme.getInstance()));
        List<MessageConnector> connectors = MessageConnectors.createMessageConnectors(adapter, port, QDStats.VOID);
        MessageConnectors.startMessageConnectors(connectors);
        QDDistributor distributor = ticker.distributorBuilder().build();
        distributor.getAddedRecordProvider().setRecordListener(provider -> {
            RecordBuffer buffer = new RecordBuffer();
            provider.retrieve(buffer);
            while (buffer.hasNext()) {
                RecordCursor record = buffer.next();
                subscribed.add(record.getSymbol());
            }
        });
        new Thread(() -> {
            DataRecord record = DXFeedScheme.getInstance().findRecordByName("Trade");
            SymbolCodec symbolCodec = DXFeedScheme.getInstance().getCodec();
            QDDistributor qdDistributor = ticker.distributorBuilder().build();
            while (true) {
                RecordBuffer buffer = new RecordBuffer(RecordMode.DATA);
                for (String symbol : subscribed) {
                    buffer.add(record, symbolCodec.encode(symbol), symbol);
                }
                qdDistributor.process(buffer);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        Thread.sleep(Long.MAX_VALUE);
    }
}
