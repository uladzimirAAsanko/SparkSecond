import by.sanko.spark.entity.HotelData;
import by.sanko.spark.parser.HotelParser;
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
    private static String offset = "offsetConf";
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    private static HashMap<Long, HotelData> hotelData = new HashMap<>();

    public static void main(String[] args) {
        invokeHotelData();
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
        Dataset<Row> finalUsersDF = usersDF;
        ArrayList<Long> notOne = new ArrayList<>();
        notOne.add(197568495617L);
        notOne.add(206158430210L);
        notOne.add(2302102470656L);
        notOne.add(2662879723520L);
        notOne.add(3058016714753L);
        notOne.add(3100966387715L);

        notOne.forEach(s-> {
            ArrayList<String> list = new ArrayList<>();
            List<String> values = finalUsersDF.selectExpr("CAST(srch_ci AS STRING)").
                    where("hotel_id=" + s).
                    orderBy("srch_ci").
                    as(Encoders.STRING()).
                    filter((FilterFunction<String>) Objects::nonNull).
                    dropDuplicates().
                    collectAsList();
            i.getAndIncrement();
            String prevVal = values.get(0);
            System.out.println("Process " + s +" by count is " +i + " size of dates is " + values.size());
            if(values.size() != 92) {
                for (String data : values) {
                    try {
                        Date prevDate = format.parse(prevVal);
                        Date currDate = format.parse(data);
                        long diff = currDate.getTime() - prevDate.getTime();
                        long dayDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                        if (dayDiff > 1 && dayDiff < 30) {
                            System.out.println("Hotel with missing date  is " + s);
                            Calendar c = Calendar.getInstance();
                            c.setTime(prevDate);
                            c.add(Calendar.DATE, 1);
                            while (c.getTime().before(currDate)) {
                                String tmp = format.format(c.getTime());
                                System.out.println("Missing date is " + tmp);
                                list.add(tmp);
                                c.add(Calendar.DATE, 1);
                            }
                        }
                        prevVal = data;
                    } catch (ParseException e) {
                        System.out.println("Error in parsing dates");
                    } catch (NullPointerException e) {
                        System.out.println("Exception in hotel " + s);
                        System.out.println("Values are  " + values.size());
                    }
                }
            }
            listHashMap.put(s, list);
        });
        ArrayList<Long> wasted = new ArrayList<>();
        for(Long hotelID : notOne){
            ArrayList<String> list = listHashMap.get(hotelID);
            if(list != null && list.size() > 0 && list.size() < 30){
                wasted.add(hotelID);
                StringBuilder tmp = new StringBuilder(hotelID + " ");
                HotelData hotelInfo = hotelData.get(hotelID);
                tmp.append(hotelInfo.getName()).append(" ").append(hotelInfo.getCountry()).append(" ")
                        .append(hotelInfo.getCity()).append(" ").append(hotelInfo.getAddress());
                for(String value : list){
                    tmp.append(value).append(" ");
                }
                System.out.println(tmp.toString());
            }
        }
        for(Long val : wasted){
            usersDF = usersDF.where("hotel_id!=" + val);
        }
        usersDF.write().format("csv")
                .partitionBy("srch_ci")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .save("/user/hadoop/task1/expedia/new_ver/");
        System.out.println("Hotels are " + hotelsID.size());
        System.out.println("Searched val " + hotelsID.get(1));
        System.out.println("Select all ");
    }

    private static void invokeHotelData(){
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        ResourceBundle resourceBundle = ResourceBundle.getBundle(offset);
        resourceBundle.getString("endingOffsets");
        Dataset<Row> df = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host.docker.internal:9094")
                .option("subscribe", "hw-data-topic") //weathers-data-hash
                .option("endingOffsets", resourceBundle.getString("endingOffsets"))
                .option("startingOffsets", resourceBundle.getString("startingOffsets"))
                .option("maxOffsetsPerTrigger", resourceBundle.getString("maxOffsetsPerTrigger"))
                .load();
        spark.sparkContext().setLogLevel("ERROR");
        List<String> stringList = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING()).collectAsList();
        List<String> hotels = new ArrayList<>();
        for(String value : stringList){
            int index = value.indexOf('\n');
            String tmp = value.substring(index + 1, value.indexOf('\n', index +1));
            hotels.add(tmp);
            System.out.println(tmp);
        }
        for(String hotel : hotels){
            HotelData data = HotelParser.parseData(hotel);
            hotelData.put(data.getId(), data);
        }
        System.out.println("Hotel data is " + hotelData.size());
        long numAs = df.count();
        System.out.println("Lines at all: " + numAs);
    }
}
