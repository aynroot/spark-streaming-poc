package io.relayr.analytics.people;

import io.relayr.analytics.people.entities.EnhancedPerson;
import io.relayr.analytics.people.entities.Person;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;

import static org.apache.spark.sql.functions.*;

public class StructuredStreamingPOC {
    private static StructType personSchema = new StructType()
            .add("name", "string", false)
            .add("sex", "string", false)
            .add("age", "integer", false)
            .add("timestamp", "timestamp", false);

    public static void main(String[] args) throws Exception {
        HashMap<String, Integer> coarseGrainSteps = new HashMap<>();
        coarseGrainSteps.put("m", 5000);
        coarseGrainSteps.put("f", 10000);
        HashMap<String, String> aggregations = new HashMap<>();
        aggregations.put("m", "latest");
        aggregations.put("f", "average");
        HashMap<String, Integer> watermarks = new HashMap<>();
        watermarks.put("m", 20);
        watermarks.put("f", 30);
//        for (int i = 0; i < 100000; i++) {
//            watermarks.put(String.format("z%s", i), i);
//            aggregations.put(String.format("z%s", i), "latest");
//            coarseGrainSteps.put(String.format("z%s", i), 1000);
//        }

        SparkSession spark = getSparkSession();
        spark.sparkContext().setLogLevel("WARN");

        // 3 - Create a Dataset representing the stream of input files
        Dataset<Row> rawStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()
                .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json_value");
        rawStream.printSchema();

        Dataset<Person> peopleStream = rawStream
                .select(from_json(rawStream.col("json_value"), personSchema).as("person"))
                .select("person.*")
                .as(Encoders.bean(Person.class));
        peopleStream.printSchema();

        // enhance people with their coarse grains and aggregation types
        Dataset<EnhancedPerson> enhancedPeopleStream = peopleStream.map((MapFunction<Person, EnhancedPerson>) rawPerson -> {
            EnhancedPerson person = new EnhancedPerson(rawPerson);
            person.setCoarseGrainStep(coarseGrainSteps.get(person.getSex()));
            person.setAggregationType(aggregations.get(person.getSex()));

            Integer coarseGrainStep = coarseGrainSteps.get(person.getSex());
            person.setCoarseGrainedTimestamp(Timestamp.from(Instant.ofEpochMilli(
                    person.getTimestamp().getTime() / coarseGrainStep * coarseGrainStep
            )));
            return person;
        }, Encoders.bean(EnhancedPerson.class));


        // NB: we can only create N streams for all possible values of 'sex' which can lead to too many states
        // we can not get sex values from the current batch / stream and only process those :/
        // that's a no go if we have thousands of groups (even if there is no data for them, the states are created)
        // data from kafka is read all those number of times
        for (String sex : watermarks.keySet()) {
            Dataset<EnhancedPerson> watermarkedPeople = enhancedPeopleStream
                    .filter((FilterFunction<EnhancedPerson>) person -> person.getSex() != null && person.getSex().equals(sex))
                    .withWatermark("timestamp", String.format("%s seconds", watermarks.get(sex)));

            Dataset<Row> ages;
            if (aggregations.get(sex).equals("latest")) {
                ages = watermarkedPeople
                        .select(col("timestamp"), struct(col("timestamp"), col("age")).as("timestampedAge"), col("coarseGrainedTimestamp"))
                        .groupBy(window(col("timestamp"), String.format("%s milliseconds", coarseGrainSteps.get(sex))),
                                col("coarseGrainedTimestamp"))
                        .agg(max("timestampedAge").as("latestAge"))
                        .select(struct(col("coarseGrainedTimestamp"), col("latestAge.age").cast("double").as("aggregated age")).as("value"));
            } else {
                ages = watermarkedPeople
                        .groupBy(window(col("timestamp"), String.format("%s milliseconds", coarseGrainSteps.get(sex))),
                                col("coarseGrainedTimestamp"))
                        .agg(avg("age").as("avgAge"))
                        .select(struct(col("coarseGrainedTimestamp"), col("avgAge").as("aggregated age")).as("value"));
            }

            ages.writeStream()
                    .outputMode(OutputMode.Append())
                    .format("console")
                    .option("truncate", false)
                    .queryName(sex)
                    .start();
        }

        // NB: unionizing doesn't work due to
        // "Multiple streaming aggregations are not supported with streaming DataFrames/Datasets"
        /*
        Dataset<Row> dataset = datasets.get(0);
        for (Dataset<Row> ds : datasets.subList(1, datasets.size())) {
            dataset = dataset.union(ds);
        }
        dataset.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .option("truncate", false)
                .queryName("test")
                .start();
        */

        spark.streams().awaitAnyTermination();
    }

    private static SparkSession getSparkSession() {
        return SparkSession
                .builder().config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.streaming.checkpointLocation", "checkpoints")
                .master("local[*]")
                .appName("StructuredStreamingPOC")
                .getOrCreate();
    }
}
