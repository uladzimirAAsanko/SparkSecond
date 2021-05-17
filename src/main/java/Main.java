import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
    }
}
