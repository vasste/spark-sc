package org.spark.qds.sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.BucketSpec;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class QDDataSourceReader extends DataSource {

    public QDDataSourceReader(SparkSession sparkSession, String className, Seq<String> paths,
                              Option<StructType> userSpecifiedSchema, Seq<String> partitionColumns,
                              Option<BucketSpec> bucketSpec, Map<String, String> options, Option<CatalogTable> catalogTable) {
        super(sparkSession, className, paths, userSpecifiedSchema, partitionColumns, bucketSpec, options, catalogTable);
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }
}
