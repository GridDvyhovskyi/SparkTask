package com.grid;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.col;

import java.util.*;


public class MetricsCalculator {

    private static Dataset<Row> prefix(Dataset<Row> dataset, String prefix) {
        for(String col: dataset.columns()) dataset = dataset.withColumnRenamed(col, col + prefix);
        return dataset;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: com.grid.MetricsCalculator <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("MetricsCalculator Task").setMaster("local");
        SparkContext sparkContext = new SparkContext(conf);
        SparkSession spark = SparkSession.builder().
                sparkContext(sparkContext)
                .master("local")
                .appName("com.grid.MetricsCalculator")
                .getOrCreate();
        StructType customSchema = new StructType(new StructField[] {
                new StructField("script_version", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("host", DataTypes.StringType, true, Metadata.empty()),
                new StructField("uuid", DataTypes.StringType, true, Metadata.empty()),
                new StructField("basic_metric", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("metrics", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> initial_metrics = spark.read()
                .option("header", "false")
                .option("delimiter","\t")
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
                .schema(customSchema)
                .csv(args[0]);

        Dataset<Row> current_metrics = spark.read()
                .option("header", "false")
                .option("delimiter","\t")
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
                .schema(customSchema)
                .csv(args[1]);

        Dataset<Row> joinedMetrics = prefix(initial_metrics, "_1").join(prefix(current_metrics, "_2"),
                (column("host_1").equalTo(col("host_2")).
                        and(column("uuid_1").equalTo(column("uuid_2")))), "inner").
                orderBy(column("host_1"));

        UDF2 compareMetrics = new UDF2<String, String, String>() {
            public String call(final String col1, final String col2) throws Exception {
                Map<String, Integer> initialMetricsMap = new TreeMap<>();
                String[] initialMetrics = col1.replaceAll("[{}'\\s+]", "").split(",");
                for(String metric: initialMetrics){
                    String[] splitMetric = metric.split(":");
                  initialMetricsMap.put(splitMetric[0], Integer.valueOf(splitMetric[1]));
               }
                Map<String, Integer> currentMetricsMap = new TreeMap<>();
                String[] currentMetrics = col2.replaceAll("[{}'\\s+]", "").split(",");
                for(String metric: currentMetrics){
                    String[] splitMetric = metric.split(":");
                    currentMetricsMap.put(splitMetric[0], Integer.valueOf(splitMetric[1]));
                }
                Map<String, Integer> regressionMetricsMap = new TreeMap<>();

                for(String metricID: initialMetricsMap.keySet()) {
                    if (currentMetricsMap.containsKey(metricID)) {
                        if ((currentMetricsMap.get(metricID) - initialMetricsMap.get(metricID)) % initialMetricsMap.get(metricID) >
                                initialMetricsMap.get(metricID) * 0.1) {
                            regressionMetricsMap.put(metricID, currentMetricsMap.get(metricID) - initialMetricsMap.get(metricID));
                        }
                    }
                }
                return regressionMetricsMap.keySet().isEmpty() ? null : regressionMetricsMap.toString();
            }
        };
        spark.udf().register("compareMetrics",compareMetrics,DataTypes.StringType);
        joinedMetrics.withColumn("regression",
                        callUDF("compareMetrics", column("metrics_1"), column("metrics_2")))
                .select("host_1", "uuid_1", "metrics_1", "metrics_2", "regression")
                .where(column("regression").isNotNull())
                .show(false);
        spark.stop();
    }


}
