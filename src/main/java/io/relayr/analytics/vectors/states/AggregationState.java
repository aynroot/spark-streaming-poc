package io.relayr.analytics.vectors.states;

import io.relayr.analytics.vectors.Mappings;
import io.relayr.analytics.vectors.entities.EnhancedDeviceReading;
import io.relayr.analytics.vectors.entities.MetaData;
import io.relayr.analytics.vectors.entities.Vector;
import lombok.Data;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static com.google.common.math.DoubleMath.mean;

public class AggregationState implements Serializable {
    private final String vectorId;

    private Timestamp maxSeenEventTime = new Timestamp(0);
    private TreeSet<Timestamp> bufferedTimestamps = new TreeSet<>();

    private HashMap<Timestamp, HashMap<String, List<Double>>> averagePools = new HashMap<>();
    private HashMap<Timestamp, HashMap<String, Tuple2<Timestamp, Double>>> latestValues = new HashMap<>();

    public AggregationState(String vectorId) {
        this.vectorId = vectorId;
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
                                    mean(averagePools.get(timestamp).get(feature)));
                }
            }

            // clean up buffers
            latestValues.remove(timestamp);
            averagePools.remove(timestamp);
            bufferedTimestamps.remove(timestamp);
            return this;
        }
    }
}
