package org.qds;

import com.dxfeed.api.DXEndpoint;
import com.dxfeed.api.DXPublisher;
import com.dxfeed.api.osub.ObservableSubscription;
import com.dxfeed.api.osub.ObservableSubscriptionChangeListener;
import com.dxfeed.event.market.Trade;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class TradesFeed {
    public static void main(String[] args) throws InterruptedException {
        DXEndpoint dxEndpoint = DXEndpoint.getInstance(DXEndpoint.Role.PUBLISHER);
        DXPublisher dxPublisher = dxEndpoint.getPublisher();
        dxEndpoint.connect(":7099");
        while (true) {
            Trade trade = new Trade("AAPL");
            trade.setTime(System.currentTimeMillis());
            trade.setPrice(ThreadLocalRandom.current().nextDouble() * 100);
            dxPublisher.publishEvents(Collections.singleton(trade));
            Thread.sleep(1000);
        }
    }
}
