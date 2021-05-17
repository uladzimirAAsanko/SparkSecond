import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.round;

public class Main {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
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
        List<Long> hotelsID =  usersDF.selectExpr("CAST(hotel_id AS LONG)").as(Encoders.LONG()).collectAsList();//Long.parseLong(row.toString())) hotels_id.add(row.getLong(0)
        HashSet<Long> longs = new HashSet<>(hotelsID);
        HashMap<Long,ArrayList<String>> listHashMap = new HashMap<>();
        AtomicInteger i = new AtomicInteger();
        System.out.println("Uniq hotels are " + longs.size());
        longs.forEach(s-> {
            ArrayList<String> list = new ArrayList<>();
            List<String> values = usersDF.selectExpr("CAST(srch_ci AS STRING)").
                    where("hotel_id=" + s).
                    orderBy("srch_ci").
                    as(Encoders.STRING()).
                    filter((FilterFunction<String>) Objects::nonNull).
                    collectAsList();
            i.getAndIncrement();
            String prevVal = values.get(0);
            Date prevDatetmp = null;
            try {
                prevDatetmp = format.parse(values.get(0));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date currDatetmp = null;
            try {
                currDatetmp = format.parse(values.get(1));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long difftmp = prevDatetmp.getTime() - currDatetmp.getTime();
            long dayDifftmp = TimeUnit.DAYS.convert(difftmp, TimeUnit.MILLISECONDS);
            System.out.println("Process " + s +" by count is " +i + " size of dates is " + values.size() + " first value " + prevDatetmp + " second value " + currDatetmp + " daydiff " + dayDifftmp);
            for(String data : values){
                try {
                    Date prevDate = format.parse(prevVal);
                    Date currDate = format.parse(data);
                    long diff = prevDate.getTime() - currDate.getTime();
                    long dayDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                    if (dayDiff > 1){
                        System.out.println("Hotel with missing date  is " + s);
                        Calendar c = Calendar.getInstance();
                        c.setTime(prevDate);
                        c.add(Calendar.DATE,1);
                        while(c.getTime().before(currDate)){
                            String tmp = format.format(c.getTime());
                            System.out.println("Missing date is " + tmp);
                            list.add(tmp);
                            c.add(Calendar.DATE,1);
                        }
                    }
                    prevVal = data;
                } catch (ParseException e) {
                    System.out.println("Error in parsing dates");
                }
                catch (NullPointerException e){
                    System.out.println("Exception in hotel " + s);
                    System.out.println("Values are  " + values.size());
                }
            }
            listHashMap.put(s, list);
        });

        System.out.println("Hotels are " + hotelsID.size());
        System.out.println("Searched val " + hotelsID.get(1));
        System.out.println("Select all ");
    }
}
