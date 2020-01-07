package io.relayr.analytics.people;

import com.google.common.collect.Lists;
import io.relayr.analytics.people.entities.EnhancedPerson;
import io.relayr.analytics.people.entities.Person;
import io.relayr.analytics.people.states.PeopleAggregationState;
import io.relayr.analytics.people.states.PeopleOrderingState;
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
import scala.Tuple3;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

public class StructuredStreamingPOC2 {
    private static StructType personSchema = new StructType()
            .add("name", "string", false)
            .add("sex", "string", false)
            .add("age", "integer", false)
            .add("timestamp", "timestamp", false);

    public static void main(String[] args) throws Exception {
        HashMap<String, Integer> coarseGrainSteps = new HashMap<>();
        coarseGrainSteps.put("m", 5000);
        coarseGrainSteps.put("f", 10000);

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
            person.setReceivedTimestamp(Timestamp.from(Instant.now()));

            Integer coarseGrainStep = coarseGrainSteps.get(person.getSex());
            person.setCoarseGrainStep(coarseGrainStep);
            person.setCoarseGrainedTimestamp(Timestamp.from(Instant.ofEpochMilli(
                    person.getTimestamp().getTime() / coarseGrainStep * coarseGrainStep
            )));
            return person;
        }, Encoders.kryo(EnhancedPerson.class));

        // NB: we need to use states to allow per-key watermarking ( = n coarse grain steps to accumulate )
        Dataset<Tuple3<String, Timestamp, Double>> aggregatedStream = enhancedPeopleStream
                .groupByKey((MapFunction<EnhancedPerson, String>) Person::getSex, Encoders.STRING())
                .flatMapGroupsWithState(
                        StructuredStreamingPOC2.aggregationSpec(),
                        OutputMode.Append(),
                        Encoders.kryo(PeopleAggregationState.class),
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.DOUBLE()),
                        GroupStateTimeout.ProcessingTimeTimeout()
                );

        Dataset<Tuple3<String, Timestamp, Integer>> orderedStream = aggregatedStream
                .groupByKey((MapFunction<Tuple3<String, Timestamp, Double>, String>) Tuple3::_1, Encoders.STRING())
                .flatMapGroupsWithState(
                        // by this point we can guaranteed that the micro-batches coming from the aggregation
                        // have an ascending order inside
                        // but since everything gets reshuffled, we need to retain the order by sorting iterator's data
                        StructuredStreamingPOC2.orderingSpec(),
                        OutputMode.Append(),
                        Encoders.kryo(PeopleOrderingState.class),
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.INT()),
                        GroupStateTimeout.ProcessingTimeTimeout()
                );

        orderedStream
                .writeStream()
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .format("console")
                .option("truncate", false)
                .queryName("poc2")
                .start()
                .awaitTermination();
    }

    private static FlatMapGroupsWithStateFunction<String, EnhancedPerson, PeopleAggregationState, Tuple3<String, Timestamp, Double>> aggregationSpec() {
        return (FlatMapGroupsWithStateFunction<String, EnhancedPerson, PeopleAggregationState, Tuple3<String, Timestamp, Double>>)
                (key, valuesIterator, state) -> {
                    System.out.println(String.format("sex = %s | Current processing time: %s",
                            key, new Timestamp(state.getCurrentProcessingTimeMs())));

                    if (state.hasTimedOut()) {
                        System.out.println(String.format("sex = %s state is timing out.", key));
                        List<Tuple3<String, Timestamp, Double>> results = state.get().handleTimeout();
                        state.remove();
                        return results.iterator();
                    }

                    PeopleAggregationState prevState = state.exists() ? state.get() : new PeopleAggregationState(key);
                    // values in `values` iterator come in random order
                    // need to presort according to the received timestamp
                    // to maintain order per "device" in a group's micro-batch
                    List<EnhancedPerson> values = Lists.newArrayList(valuesIterator);
                    values.sort(Comparator.comparing(EnhancedPerson::getReceivedTimestamp));
                    values.forEach(person -> {
                        long watermark = prevState.update(person);
                        state.setTimeoutDuration(watermark);
                    });
                    List<Tuple3<String, Timestamp, Double>> result = prevState.getNewAggregatedPeopleStats();

                    state.update(prevState);
                    return result.iterator();
                };
    }

    private static FlatMapGroupsWithStateFunction<String, Tuple3<String, Timestamp, Double>, PeopleOrderingState, Tuple3<String, Timestamp, Integer>> orderingSpec() {
        return (FlatMapGroupsWithStateFunction<String, Tuple3<String, Timestamp, Double>, PeopleOrderingState, Tuple3<String, Timestamp, Integer>>)
                (key, valuesIterator, state) -> {
                    if (state.hasTimedOut()) {
                        System.out.println(String.format("ordering state for sex = %s state is timing out, cleaning up.", key));
                        state.remove();
                        return Collections.emptyIterator();
                    }

                    PeopleOrderingState prevState = state.exists() ? state.get() : new PeopleOrderingState();
                    // values in `values` iterator come in random order
                    // need to presort according to the timestamp (coarse grained)
                    // to maintain order per "device" in a group's micro-batch
                    List<Tuple3<String, Timestamp, Double>> values = Lists.newArrayList(valuesIterator);
                    List<Tuple3<String, Timestamp, Integer>> results = new ArrayList<>();
                    values.sort(Comparator.comparing(Tuple3::_2));
                    values.forEach(tuple -> {
                        int index = prevState.getNextIndex();
                        results.add(new Tuple3<>(tuple._1(), tuple._2(), index));
                        state.setTimeoutDuration("5 minutes");
                    });

                    state.update(prevState);
                    return results.iterator();
                };
    }


    private static SparkSession getSparkSession() {
        return SparkSession
                .builder().config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
//                .config("spark.sql.streaming.checkpointLocation", "checkpoints")
                .config("spark.sql.shuffle.partitions", "8")
                .master("local[*]")
                .appName("StructuredStreamingPOC")
                .getOrCreate();
    }
}
