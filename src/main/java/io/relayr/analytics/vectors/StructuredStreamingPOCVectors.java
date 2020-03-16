package io.relayr.analytics.vectors;

import com.google.common.collect.Lists;
import io.relayr.analytics.vectors.entities.DeviceReading;
import io.relayr.analytics.vectors.entities.EnhancedDeviceReading;
import io.relayr.analytics.vectors.entities.MetaData;
import io.relayr.analytics.vectors.entities.Vector;
import io.relayr.analytics.vectors.states.AggregationState;
import io.relayr.analytics.vectors.states.AugmentationState;
import org.apache.spark.api.java.function.FlatMapFunction;
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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class StructuredStreamingPOCVectors {
    private static StructType readingSchema = new StructType()
            .add("name", "string", false)
            .add("device", "string", false)
            .add("value", "double", false)
            .add("timestamp", "timestamp", false);

    // TODO: data validation on input: how to do?

    /*
    NOT ALLOWED
    - Changes in the number or type (i.e. different source) of input sources
    - Changes to subscribed topics/files
    - Any changes (that is, additions, deletions, or schema modifications) to the stateful operations of a streaming query
      - Streaming aggregation: For example, sdf.groupBy("a").agg(...). Any change in number or type of grouping keys or aggregates is not allowed.
      - Streaming deduplication: For example, sdf.dropDuplicates("a"). Any change in number or type of grouping keys or aggregates is not allowed.
      - Stream-stream join: For example, sdf1.join(sdf2, ...) (i.e. both inputs are generated with sparkSession.readStream).
        Changes in the schema or equi-joining columns are not allowed.
        Changes in join type (outer or inner) not allowed.
        Other changes in the join condition are ill-defined.
      - Arbitrary stateful operation: For example, sdf.groupByKey(...).mapGroupsWithState(...) or sdf.groupByKey(...).flatMapGroupsWithState(...).
        Any change to the schema of the user-defined state and the type of timeout is not allowed.
        Any change within the user-defined state-mapping function are allowed, but the semantic effect of the change depends on the user-defined logic.
        If you really want to support state schema changes, then you can explicitly encode/decode your complex state data structures into bytes using an encoding/decoding scheme that supports schema migration.
        For example, if you save your state as Avro-encoded bytes, then you are free to change the Avro-state-schema between query restarts as the binary state will always be restored successfully.
    - Output sink changes: Kafka sink to file sink is not allowed.
    - Changes to output directory of a file sink is not allowed: sdf.writeStream.format("parquet").option("path", "/somePath") to sdf.writeStream.format("parquet").option("path", "/anotherPath")

    ALLOWED:
    - Addition / deletion of filters is allowed
    - Changes in projections with same output schema is allowed: sdf.selectExpr("stringColumn AS json").writeStream to sdf.selectExpr("anotherStringColumn AS json").writeStream
    - Addition/deletion/modification of rate limits is allowed: spark.readStream.format("kafka").option("subscribe", "topic") to spark.readStream.format("kafka").option("subscribe", "topic").option("maxOffsetsPerTrigger", ...)
    - _Some_ changes in the type of output sink:
      - File sink to Kafka sink is allowed. Kafka will see only the new data.
      - Kafka sink changed to foreach, or vice versa is allowed.
      - Changes to output topic is allowed: sdf.writeStream.format("kafka").option("topic", "someTopic") to sdf.writeStream.format("kafka").option("topic", "anotherTopic")

    CONDITIONALLY ALLOWED:
    - Changes in projections with different output schema are conditionally allowed: sdf.selectExpr("a").writeStream to sdf.selectExpr("b").writeStream is allowed only if the output sink allows the schema change from "a" to "b".
    - Changes to the user-defined foreach sink (that is, the ForeachWriter code) is allowed, but the semantics of the change depends on the code.
     */

    public static void main(String[] args) throws Exception {
        SparkSession spark = getSparkSession();
        spark.sparkContext().setLogLevel("WARN");

        spark.streams().addListener(new QueryListener());

        Dataset<Row> rawStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()
                .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json_value", "timestamp");
        rawStream.printSchema();

        Dataset<DeviceReading> rawReadings = rawStream
                .select(from_json(rawStream.col("json_value"), readingSchema).as("reading"), col("timestamp").as("kafkaTimestamp"))
                .select("reading.*", "kafkaTimestamp")
                .as(Encoders.bean(DeviceReading.class));
        rawReadings.printSchema();

        // enhance readings with their vectors, coarse grained timestamps and received timestamps
        Dataset<EnhancedDeviceReading> vectorizedReadings = rawReadings.flatMap((FlatMapFunction<DeviceReading, EnhancedDeviceReading>) reading -> {
            List<EnhancedDeviceReading> enhancedReadings = new ArrayList<>();
            for (String vectorId : Mappings.deviceToVectors.get(reading.getDevice()).split(",")) {
                Integer coarseGrainStep = Mappings.featuresCoarseGrainSteps.get(reading.getName());
                EnhancedDeviceReading enhancedReading = EnhancedDeviceReading.builder()
                        .reading(reading)
                        .vectorId(vectorId)
                        .receivedTimestamp(new Timestamp(reading.getKafkaTimestamp()))
                        .coarseGrainedTimestamp(Timestamp.from(Instant.ofEpochMilli(
                                reading.getTimestamp().getTime() / coarseGrainStep * coarseGrainStep
                        )))
                        .build();
                enhancedReadings.add(enhancedReading);
            }
            return enhancedReadings.iterator();
        }, Encoders.kryo(EnhancedDeviceReading.class));

        vectorizedReadings
                .groupByKey((MapFunction<EnhancedDeviceReading, String>) EnhancedDeviceReading::getVectorId, Encoders.STRING())
                .flatMapGroupsWithState(
                        StructuredStreamingPOCVectors.aggregationSpec(),
                        OutputMode.Append(),
                        Encoders.BINARY(),
                        Encoders.tuple(Encoders.kryo(MetaData.class), Encoders.kryo(Vector.class)),
                        GroupStateTimeout.ProcessingTimeTimeout()
                )
                .groupByKey((MapFunction<Tuple2<MetaData, Vector>, String>) tuple -> tuple._1.getVectorId(), Encoders.STRING())
                .flatMapGroupsWithState(
                        // by this point we can guarantee that the micro-batches coming from the aggregation
                        // have an ascending order inside
                        // but since everything gets reshuffled, we need to retain the order by sorting iterator's data
                        StructuredStreamingPOCVectors.augmentationSpec(),
                        OutputMode.Append(),
                        Encoders.kryo(AugmentationState.class),
                        Encoders.tuple(Encoders.kryo(MetaData.class), Encoders.kryo(Vector.class)),
                        GroupStateTimeout.ProcessingTimeTimeout()
                )
                .map((MapFunction<Tuple2<MetaData, Vector>, Tuple2<String, String>>) t -> new Tuple2<>(t._1.getVectorId(), t._2.toString()),
                        Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .select(col("_1").as("key"), col("_2").as("value"))
                .writeStream()
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
                .format("console")
                .option("truncate", false)
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "localhost:9092")
//                .option("topic", "test-out")
                .queryName("vectorization-v3")
                .start()
                .awaitTermination();
    }

    private static FlatMapGroupsWithStateFunction<String, EnhancedDeviceReading, byte[], Tuple2<MetaData, Vector>> aggregationSpec() {
        return (FlatMapGroupsWithStateFunction<String, EnhancedDeviceReading, byte[], Tuple2<MetaData, Vector>>)
                (vectorId, valuesIterator, sparkState) -> {
                    System.out.println(String.format("%s | Current processing time: %s",
                            vectorId, new Timestamp(sparkState.getCurrentProcessingTimeMs())));

                    if (sparkState.hasTimedOut()) {
                        System.out.println(String.format("%s aggregation state is timing out.", vectorId));

                        List<Tuple2<MetaData, Vector>> results = AggregationState.deserialize(sparkState.get()).handleTimeout();
                        sparkState.remove();
                        return results.iterator();
                    }

                    AggregationState state = sparkState.exists() ? AggregationState.deserialize(sparkState.get()) : new AggregationState(vectorId);
                    // values in `values` iterator come in random order
                    // need to presort according to the received timestamp
                    // to maintain order per device in a group's micro-batch
                    List<EnhancedDeviceReading> values = Lists.newArrayList(valuesIterator);

                    // check that values indeed came unsorted, and we need to resort
                    double prevValue = -1.0;
                    for (EnhancedDeviceReading value : values) {
                        if (value.getReading().getKafkaTimestamp() <= prevValue) {
                            System.out.println(String.format("AGGREGATION: unsorted iterator (len(values) == %d)", values.size()));
                        }
                        prevValue = value.getReading().getKafkaTimestamp();
                    }

                    values.sort(Comparator.comparing(EnhancedDeviceReading::getReceivedTimestamp));

                    values.forEach(reading -> {
                        state.update(reading);
                        sparkState.setTimeoutDuration(Mappings.vectorsWatermarks.get(vectorId));
                    });
                    List<Tuple2<MetaData, Vector>> result = state.getAggregatedVectors();

                    sparkState.update(state.serialize());
                    return result.iterator();
                };
    }

    private static FlatMapGroupsWithStateFunction<String, Tuple2<MetaData, Vector>, AugmentationState, Tuple2<MetaData, Vector>> augmentationSpec() {
        return (FlatMapGroupsWithStateFunction<String, Tuple2<MetaData, Vector>, AugmentationState, Tuple2<MetaData, Vector>>)
                (vectorId, valuesIterator, sparkState) -> {
                    if (sparkState.hasTimedOut()) {
                        System.out.println(String.format("augmentation state for %s state is timing out, cleaning up.", vectorId));
                        sparkState.remove();
                        return Collections.emptyIterator();
                    }

                    AugmentationState augmentationState = sparkState.exists() ? sparkState.get() : new AugmentationState();
                    // values in `values` iterator come in random order
                    // need to presort according to the timestamp (coarse grained)
                    // to maintain order per vector in a group's micro-batch
                    List<Tuple2<MetaData, Vector>> values = Lists.newArrayList(valuesIterator);
                    List<Tuple2<MetaData, Vector>> results = new ArrayList<>();
                    // here we have presorted values, probably because the stream is partitioned by the same key as before
                    // we still should perform sorting, since the order is not guaranteed
                    values.sort(Comparator.comparing(o -> o._2.getCoarseGrainedTimestamp()));
                    values.forEach(tuple -> {
                        results.add(new Tuple2<>(tuple._1(), augmentationState.getAugmentedVector(tuple._2)));
                        sparkState.setTimeoutDuration("5 minutes");
                    });

                    sparkState.update(augmentationState);
                    return results.iterator();
                };
    }

    private static SparkSession getSparkSession() {
        return SparkSession.builder()
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.streaming.metricsEnabled", "true")
                .config("spark.sql.streaming.minBatchesToRetain", 360) // desired retention = 1 hour
                .config("spark.sql.streaming.checkpointLocation", "checkpoints")
                .config("spark.sql.shuffle.partitions", "8")
                .master("local[*]")
                .appName("StructuredStreamingPOC")
                .getOrCreate();
    }
}
