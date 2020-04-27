package com.SparkSql;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;

/**
 * Hello world!
 *
 */
public class SparkSQL 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession
        		.builder()
        		.master("local")
        		.appName("Spark SQL Examples")
        		.getOrCreate();
        
        Dataset<Row> jdbcDF = spark.read()
        		  .format("jdbc")
        		  .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
        		  .option("dbtable", "public.people")
        		  .option("user", "postgres")
        		  .option("password", "qwerty1234")
        		  .load();
        
        StructType schema = new StructType()
        		.add("name", "string")
        		.add("age", "long")
        		.add("job", "string");
        
        Dataset<Row> df = spark.read()
        		.option("mode", "DROPMALFORMED")
        		.option("delimiter", ";")
        		.option("header", "true")
        		.schema(schema)
        		.csv("G:\\Bahan ngajar\\people.csv");
        
        Dataset<Row> df1 = spark.read()
        		.json("G:\\Bahan ngajar\\people.json");
        
//        jdbcDF.show();
        Dataset<Row> resJdbcDF = jdbcDF.groupBy("job").count().select(col("job"), col("count").alias("result"));
        
//        String result = resJdbcDF.toJSON().collectAsList().toString();
        
        MongoSpark.write(resJdbcDF)
        .option("spark.mongodb.output.uri", "mongodb://localhost/spark.people")
        .mode("append")
        .save();
//        resJdbcDF.write()
//        .mode("append")
//        .format("jdbc")
//        .option("url", "jdbc:postgresql://127.0.0.1:5432/postgres")
//        .option("dbtable", "public.results")
//        .option("user", "postgres")
//        .option("password", "qwerty1234")
//        .save();
//        df.show();
//        df.select("name").show();
//        df.filter(col("age").gt(21)).show();
//        df.groupBy("age").count().show();
    }
}
