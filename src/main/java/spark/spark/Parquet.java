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
		//csv.printSchema();
		
		//csv.write().save("C:\\\\\\\\Users\\\\\\\\Pratik Joshi\\\\\\\\Desktop\\\\\\\\spark\\\\\\\\weather1");
		//csv.write().mode("overwrite").  parquet("C:\\\\Users\\\\Pratik Joshi\\\\Desktop\\\\spark\\\\weather");
		//csv.write().format("parquet").mode("overwrite"). saveAsTable("weather");
		/*String end = new SimpleDateFormat("HHmmss").format(Calendar.getInstance().getTime());
		
		Dataset<Row> sql = session.sql("select * from weather where YR='41'");
		
		System.out.println("count: "+sql.count());
		
		
		System.out.println(start+" "+end);
		System.out.println(Integer.parseInt(end)-Integer.parseInt(start) );
		System.out.println("Execution Completed");
		
		
		
		Dataset<customerPoJo> csv2 = session.read(). csv("C:\\\\Users\\\\Pratik Joshi\\\\Desktop\\\\spark\\\\sampleFile.csv").map(row -> {
			return new customerPoJo(row.getString(0), row.getString(1), row.getString(2), row.getString(3));}
		, Encoders.bean(customerPoJo.class));
		
		csv2.write().option("compression", "uncompressed").mode("overwrite").save("c:\\Users\\Pratik Joshi\\Desktop\\spark\\datalzo");
		
		System.out.println("execution completed");
		
		Dataset<Row> parquet = session.read().parquet("c:\\Users\\Pratik Joshi\\Desktop\\spark\\datalzo");
		
	//	parquet.filter("id>2000").write().save("c:\\\\Users\\\\Pratik Joshi\\\\Desktop\\\\spark\\\\dataFilter");
		
		
		Dataset<Row> parquet2 = session.read().parquet("c:\\\\\\\\Users\\\\\\\\Pratik Joshi\\\\\\\\Desktop\\\\\\\\spark\\\\\\\\dataFilter");
		
		System.out.println("count:"+parquet2.count());
		
		parquet.show(); */

	}

}
