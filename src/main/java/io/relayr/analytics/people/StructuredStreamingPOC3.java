package io.relayr.analytics.people;

import io.relayr.analytics.people.entities.EnhancedPerson;
import io.relayr.analytics.people.entities.Person;
import io.relayr.analytics.people.states.PeopleAggregationStateSingleTimestamp;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.from_json;

public class StructuredStreamingPOC3 {

    // TODO: won't work, since there will only be data related to the current coarse grain step in the state

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
        for (int i = 0; i < 100000; i++) {
            aggregations.put(String.format("z%s", i), "latest");
            coarseGrainSteps.put(String.format("z%s", i), 1000);
        }

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


        // NB: we need to use states to allow per-key watermarking
        Dataset<Tuple3<String, Timestamp, Double>> resultsStream = enhancedPeopleStream
                .withWatermark("coarseGrainedTimestamp", "2 hours") // NB: across all groups!
                .groupByKey((MapFunction<EnhancedPerson, Tuple2<String, Timestamp>>) person -> new Tuple2<>(person.getSex(), person.getCoarseGrainedTimestamp()),
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMapGroupsWithState(
                        StructuredStreamingPOC3.spec(),
                        OutputMode.Append(),
                        Encoders.kryo(PeopleAggregationStateSingleTimestamp.class),
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.DOUBLE()),
                        GroupStateTimeout.ProcessingTimeTimeout()
                );
        resultsStream
                .na().drop()
                .writeStream()
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .format("console")
                .option("truncate", false)
                .queryName("poc3")
                .start()
                .awaitTermination();
    }

    private static FlatMapGroupsWithStateFunction<Tuple2<String, Timestamp>, EnhancedPerson, PeopleAggregationStateSingleTimestamp, Tuple3<String, Timestamp, Double>> spec() {
        return (FlatMapGroupsWithStateFunction<Tuple2<String, Timestamp>, EnhancedPerson, PeopleAggregationStateSingleTimestamp, Tuple3<String, Timestamp, Double>>)
                (key, values, state) -> {
                    System.out.println(String.format("SEX = %s, TS = %s | Current processing time: %s",
                            key._1, key._2, new Timestamp(state.getCurrentProcessingTimeMs())));

                    if (state.hasTimedOut()) {
                        System.out.println(String.format("SEX = %s, TS = %s state is timing out.", key._1, key._2));
                        List<Tuple3<String, Timestamp, Double>> results = state.get().getAggregatedValue();
                        state.remove();
                        return results.iterator();
                    }

                    PeopleAggregationStateSingleTimestamp prevState = state.exists() ?
                            state.get() :
                            new PeopleAggregationStateSingleTimestamp(key._1, key._2);

                    values.forEachRemaining(person -> {
                        long watermark = prevState.update(person);
                        state.setTimeoutDuration(watermark);
                    });

                    state.update(prevState);
                    return Collections.emptyIterator();
                };
    }


    private static SparkSession getSparkSession() {
        return SparkSession
                .builder().config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
//                .config("spark.sql.streaming.checkpointLocation", "checkpoints")
                .master("local[*]")
                .appName("StructuredStreamingPOC")
                .getOrCreate();
    }
}
