package spark.spark;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Parquet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkSession session = SparkSession.builder().   appName("Parquet").
				config("spark.hadoop.fs.s3n.access.key", "").
				config("spark.hadoop.fs.s3n.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
				config("spark.hadoop.fs.s3n.secret.key", "").
				master("local").				
				getOrCreate();
		
		Dataset<Row> csv = session.read().option("header", "true").csv("s3n://amazonsparkemr//inputFiles//Summary of Weather.csv");
		String start = new SimpleDateFormat("HHmmss").format(Calendar.getInstance().getTime());
		System.out.println(csv.count() );
		
	

	}

}
