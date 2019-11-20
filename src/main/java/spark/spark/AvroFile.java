package spark.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder().appName("Avro").master("local").getOrCreate();
		
		Dataset<Row> csv = session.read().option("header", "true").csv("/mnt/tmp/sampleFile.csv");
		
		csv.filter("id>2000").write().format("com.databricks.spark.avro").save("/mnt/tmp/spark/avro");
		
		System.out.println("Exection ends");
		
	}

}
