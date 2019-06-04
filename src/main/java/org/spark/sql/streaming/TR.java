package org.spark.sql.streaming;

import java.io.Serializable;

public class TR implements Serializable {
	private String symbol;
	private double price;
	private long time;
}
