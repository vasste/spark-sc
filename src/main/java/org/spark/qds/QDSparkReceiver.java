package org.spark.qds;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class QDSparkReceiver<T> extends Receiver<T> {

    public QDSparkReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }
}
