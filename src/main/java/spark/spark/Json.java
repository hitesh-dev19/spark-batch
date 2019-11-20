package spark.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Json {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder().appName("json").enableHiveSupport().getOrCreate();
		
		
		Dataset<Row> csv = session.read().csv("/mnt/tmp/sampleFile.csv");
		
		csv.printSchema();
		csv.show();
		csv.write().saveAsTable("HiveStore");
	}

}
