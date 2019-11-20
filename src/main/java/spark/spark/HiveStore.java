package spark.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveStore {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder().appName("json").master("local").getOrCreate();
		
		
		Dataset<Row> csv = session.read().option("header", "true").csv("/mnt/tmp/sampleFile.csv");
		
		csv.printSchema();
		csv.show();
		
		csv.write().mode("overwrite").option("spark.sql.parquet.compression.codec", "gzip").parquet("/mnt/tmp/spark/gZip");
	}

}
