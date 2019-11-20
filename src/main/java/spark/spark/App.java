package spark.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
     	org.apache.spark.sql.SparkSession session = org.apache.spark.sql.SparkSession.builder().appName("fileFormat")
    			.master("local").enableHiveSupport().  getOrCreate();
    	
    
    	Dataset<SchemaForSpark> data = session.read().csv("file:///tmp/sampleFile.csv").map(row -> {
			return new SchemaForSpark(row.getString(0), row.getString(1), row.getString(2),row.getString(3));
		}, Encoders.bean(SchemaForSpark.class));
		
		SQLContext sqlContext = session.sqlContext();
		data.show();
		
		data.printSchema();
		//System.out.println("count of data ::" + data.count());
		
		//sqlContext.createExternalTable("parquet", "C:\\Users\\Pratik Joshi\\Desktop\\spark\\outputTable");
		data.write().mode("overwrite"). partitionBy("country"). parquet("file:///tmp/spark/output");
		data.write().format("parquet").mode("overwrite").  save   ("file:///tmp/spark/sampleFileParque");
		data.write().saveAsTable("sample");
		
		Dataset<Row> sql = sqlContext.sql("select * from outputTable where country='USA' ");
		sql.show();
		System.out.println(sql.count());
		
		
		/*
		data.write().format("com.databricks.spark.avro").mode("overwrite") .save("C:\\Users\\Pratik Joshi\\Desktop\\spark\\outputAvro");
		
		Dataset<Row> load = session.read().format("com.databricks.spark.avro").load("C:\\Users\\Pratik Joshi\\Desktop\\spark\\outputAvro");
		load.filter("id>2000").write().format("com.databricks.spark.avro").saveAsTable("avroTable");
		
		Dataset<Row> sql2 = sqlContext.sql("select * from avroTable");
		
		System.out.println("avro: "+sql2.count());*/
		
    }
}
