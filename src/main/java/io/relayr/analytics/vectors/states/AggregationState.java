package io.relayr.analytics.vectors.states;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.relayr.analytics.vectors.Mappings;
import io.relayr.analytics.vectors.entities.EnhancedDeviceReading;
import io.relayr.analytics.vectors.entities.MetaData;
import io.relayr.analytics.vectors.entities.Vector;
import io.relayr.analytics.vectors.states.protos.AggregationStateProto;
import lombok.Data;
import lombok.ToString;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.math.DoubleMath.mean;

@ToString
public class AggregationState implements SerializableState {
    private final String vectorId;

    private Timestamp maxSeenEventTime = new Timestamp(0);
    private TreeSet<Timestamp> bufferedTimestamps = new TreeSet<>();

    private Map<Timestamp, Map<String, List<Double>>> averagePools = new HashMap<>();
    private Map<Timestamp, Map<String, Tuple2<Timestamp, Double>>> latestValues = new HashMap<>();

    public AggregationState(String vectorId) {
        this.vectorId = vectorId;
    }


    public AggregationState(AggregationStateProto.AggregationState proto) {
        vectorId = proto.getVectorId();
        maxSeenEventTime = new Timestamp(proto.getMaxSeenEventTime());
        bufferedTimestamps = proto.getBufferedTimestampsList().stream().map(Timestamp::new).collect(Collectors.toCollection(TreeSet::new));

        averagePools = new HashMap<>();
        for (Map.Entry<Long, AggregationStateProto.AggregationState.FeatureAveragePool> averagePoolEntry : proto.getAveragePoolsMap().entrySet()) {
            HashMap<String, List<Double>> pool = new HashMap<>();

            // converting to array list, since it has to be mutable
            // integrations tests are required
            averagePoolEntry.getValue().getPoolMap().forEach((feature, valuesPool) ->
                    pool.put(feature, new ArrayList<>(valuesPool.getValuesList())));
            averagePools.put(new Timestamp(averagePoolEntry.getKey()), pool);
        }

        latestValues = new HashMap<>();
        for (Map.Entry<Long, AggregationStateProto.AggregationState.FeatureLatestValues> latestFeatureValue : proto.getLatestValuesMap().entrySet()) {
            HashMap<String, Tuple2<Timestamp, Double>> latestFeatureValues = new HashMap<>();

            latestFeatureValue.getValue().getLatestValueMap().forEach((feature, latestValue) ->
                    latestFeatureValues.put(feature, new Tuple2<>(new Timestamp(latestValue.getTimestamp()),
                            latestValue.getValue() != null ? latestValue.getValue().getValue() : null)));
            latestValues.put(new Timestamp(latestFeatureValue.getKey()), latestFeatureValues);
        }
    }

    public void update(EnhancedDeviceReading reading) {
        // only update the buffers if device's timestamp fits in the watermarked window
        if (maxSeenEventTime.getTime() - Mappings.vectorsWatermarks.get(vectorId) * 1000 < reading.getCoarseGrainedTimestamp().getTime()) {
            if (!bufferedTimestamps.contains(reading.getCoarseGrainedTimestamp())) {
                bufferedTimestamps.add(reading.getCoarseGrainedTimestamp());
                averagePools.put(reading.getCoarseGrainedTimestamp(), new HashMap<>());
                latestValues.put(reading.getCoarseGrainedTimestamp(), new HashMap<>());
                for (String feature : Mappings.vectorsFeatures.get(vectorId).split(",")) {
                    averagePools.get(reading.getCoarseGrainedTimestamp()).put(feature, new ArrayList<>());
                    latestValues.get(reading.getCoarseGrainedTimestamp()).put(feature, new Tuple2<>(new Timestamp(0), null));
                }
            }

            String feature = reading.getReading().getName();
            Timestamp originalTimestamp = reading.getReading().getTimestamp();
            Double value = reading.getReading().getValue();
            if (!new HashSet<>(Arrays.asList(Mappings.vectorsFeatures.get(vectorId).split(","))).contains(feature)) {
                return;
            }

            if (!latestValues.containsKey(reading.getCoarseGrainedTimestamp()) ||
                    originalTimestamp.after(latestValues.get(reading.getCoarseGrainedTimestamp()).get(feature)._1)) {
                latestValues.get(reading.getCoarseGrainedTimestamp()).put(feature, new Tuple2<>(originalTimestamp, value));
            }
            averagePools.get(reading.getCoarseGrainedTimestamp()).get(feature).add(value);

            if (maxSeenEventTime.before(reading.getCoarseGrainedTimestamp()))
                maxSeenEventTime = reading.getCoarseGrainedTimestamp();
        } else {
            System.out.println(String.format("Ignoring late arrival %s %s. " +
                            "\n\tmaxSeenEventTime = %s, " +
                            "\n\ttimestamp(maxSeenEventTime.getTime() - Mappings.vectorsWatermarks.get(vectorId) * 1000) = %s\n",
                    vectorId, reading.getCoarseGrainedTimestamp(), maxSeenEventTime, new Timestamp(maxSeenEventTime.getTime() - Mappings.vectorsWatermarks.get(vectorId) * 1000)));
        }
    }

    public List<Tuple2<MetaData, Vector>> getAggregatedVectors() {
        if (!bufferedTimestamps.isEmpty()) {
            List<Tuple2<MetaData, Vector>> results = new ArrayList<>();
            while (maxSeenEventTime.getTime() - Mappings.vectorsWatermarks.get(vectorId) * 1000 >= bufferedTimestamps.first().getTime()) {
                GetEarliestAggregatedValue earliestAggregatedValue = new GetEarliestAggregatedValue().invoke();
                Timestamp timestamp = earliestAggregatedValue.getTimestamp();
                HashMap<String, Double> values = earliestAggregatedValue.getValues();

                results.add(new Tuple2<>(new MetaData(vectorId), new Vector(timestamp, values)));
            }
            return results;
        }

        return Collections.emptyList();
    }

    public List<Tuple2<MetaData, Vector>> handleTimeout() {
        List<Tuple2<MetaData, Vector>> results = new ArrayList<>();
        while (!bufferedTimestamps.isEmpty()) {
            GetEarliestAggregatedValue earliestAggregatedValue = new GetEarliestAggregatedValue().invoke();
            Timestamp timestamp = earliestAggregatedValue.getTimestamp();
            HashMap<String, Double> values = earliestAggregatedValue.getValues();

            results.add(new Tuple2<>(new MetaData(vectorId), new Vector(timestamp, values)));
        }
        return results;
    }

    @Data
    private class GetEarliestAggregatedValue {
        private Timestamp timestamp;
        private HashMap<String, Double> values = new HashMap<>();

        public GetEarliestAggregatedValue invoke() {
            timestamp = bufferedTimestamps.first();

            for (String feature : Mappings.vectorsFeatures.get(vectorId).split(",")) {
                if (Mappings.featuresAggregationTypes.get(feature).equals("latest")) {
                    values.put(feature, latestValues.get(timestamp).get(feature)._2);
                } else {
                    values.put(feature,
                            averagePools.get(timestamp).get(feature).isEmpty() ?
                                    null :
                                    mean(averagePools.get(timestamp).get(feature))
                    );
                }
            }

            // clean up buffers
            latestValues.remove(timestamp);
            averagePools.remove(timestamp);
            bufferedTimestamps.remove(timestamp);
            return this;
        }
    }

    public static AggregationState deserialize(byte[] bytes) throws InvalidProtocolBufferException {
        return new AggregationState(AggregationStateProto.AggregationState.parseFrom(bytes));
    }

    public byte[] serialize() {
        AggregationStateProto.AggregationState.Builder builder = AggregationStateProto.AggregationState.newBuilder()
                .setVectorId(vectorId)
                .setMaxSeenEventTime(maxSeenEventTime.getTime())
                .addAllBufferedTimestamps(bufferedTimestamps.stream().map(Timestamp::getTime).collect(Collectors.toList()));

        averagePools.forEach((timestamp, featuresAveragePools) ->
                builder.putAveragePools(
                        timestamp.getTime(),

                        AggregationStateProto.AggregationState.FeatureAveragePool.newBuilder()
                                .putAllPool(featuresAveragePools.entrySet().stream().map(entry ->
                                        new Tuple2<>(
                                                entry.getKey(),
                                                AggregationStateProto.AggregationState.ValuesPool.newBuilder()
                                                        .addAllValues(entry.getValue())
                                                        .build()
                                        )
                                ).collect(Collectors.toMap(t -> t._1, t -> t._2)))
                                .build()));

        latestValues.forEach((timestamp, latestFeatureValues) ->
                builder.putLatestValues(
                        timestamp.getTime(),

                        AggregationStateProto.AggregationState.FeatureLatestValues.newBuilder()
                                .putAllLatestValue(latestFeatureValues.entrySet().stream().map(entry -> {
                                            DoubleValue latestValue = entry.getValue()._2 != null
                                                    ? DoubleValue.newBuilder().setValue(entry.getValue()._2).build()
                                                    : DoubleValue.newBuilder().build();

                                            return new Tuple2<>(
                                                    entry.getKey(),
                                                    AggregationStateProto.AggregationState.LatestValue.newBuilder()
                                                            .setTimestamp(entry.getValue()._1.getTime())
                                                            .setValue(latestValue)
                                                            .build());
                                        }
                                ).collect(Collectors.toMap(t -> t._1, t -> t._2)))
                                .build()));
        return builder.build().toByteArray();
    }
}
