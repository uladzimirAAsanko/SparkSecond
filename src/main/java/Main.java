import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashSet;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> usersDF = spark.read().format("avro").load("/user/hadoop/task1/expedia/*.avro");
        String[] strings = usersDF.columns();
        System.out.println("Expedia rows are " + usersDF.count());
        System.out.println("Schema is " + usersDF.schema());
        for(String part : strings){
            System.out.println("Part is     " + part);
        }
        HashSet<Long> hotels_id = new HashSet();//Long.parseLong(row.toString())) hotels_id.add(row.getLong(0)
        usersDF.selectExpr("CAST(hotel_id AS LONG)").foreach((ForeachFunction<Row>) row -> hotels_id.add(row.getLong(0)));
        System.out.println("Hotels are " + (long) hotels_id.size());
    }
}
