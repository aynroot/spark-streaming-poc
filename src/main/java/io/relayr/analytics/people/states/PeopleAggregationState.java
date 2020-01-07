package io.relayr.analytics.people.states;

import io.relayr.analytics.people.entities.EnhancedPerson;
import lombok.Data;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static com.google.common.math.DoubleMath.mean;

@Data
public class PeopleAggregationState implements Serializable {
    private final int watermark;
    private final String sex;

    private Timestamp maxSeenEventTime = new Timestamp(0);
    private TreeSet<Timestamp> bufferedTimestamps = new TreeSet<>();

    private HashMap<Timestamp, List<Integer>> averagePools = new HashMap<>();
    private HashMap<Timestamp, Tuple2<Timestamp, Integer>> latestValues = new HashMap<>();

    public PeopleAggregationState(String sex) {
        this.sex = sex;
        watermark = sex.equals("f") ? 30 : 20;   // watermark = nCoarseGrainStepsToAccumulate * coarseGrainStep
    }

    public long update(EnhancedPerson person) {
        // only update the buffers if person's ts fits in the watermarked window
        if (maxSeenEventTime.getTime() - watermark * 1000 < person.getCoarseGrainedTimestamp().getTime()) {
            if (!bufferedTimestamps.contains(person.getCoarseGrainedTimestamp())) {
                bufferedTimestamps.add(person.getCoarseGrainedTimestamp());
                averagePools.put(person.getCoarseGrainedTimestamp(), new ArrayList<>());
            }

            if (!latestValues.containsKey(person.getCoarseGrainedTimestamp()) ||
                    person.getTimestamp().after(latestValues.get(person.getCoarseGrainedTimestamp())._1)) {
                latestValues.put(person.getCoarseGrainedTimestamp(), new Tuple2<>(person.getTimestamp(), person.getAge()));
            }
            averagePools.get(person.getCoarseGrainedTimestamp()).add(person.getAge());

            if (maxSeenEventTime.before(person.getCoarseGrainedTimestamp()))
                maxSeenEventTime = person.getCoarseGrainedTimestamp();
        } else {
            System.out.println(String.format("Ignoring late arrival %s %s", sex, person.getCoarseGrainedTimestamp()));
        }
        return watermark * 1000;
    }

    public List<Tuple3<String, Timestamp, Double>> getNewAggregatedPeopleStats() {
        if (!bufferedTimestamps.isEmpty()) {
            List<Tuple3<String, Timestamp, Double>> results = new ArrayList<>();
            while (maxSeenEventTime.getTime() - watermark * 1000 >= bufferedTimestamps.first().getTime()) {
                GetEarliestAggregatedValue earliestAggregatedValue = new GetEarliestAggregatedValue().invoke();
                Timestamp timestamp = earliestAggregatedValue.getTimestamp();
                Double aggregatedValue = earliestAggregatedValue.getValue();

                results.add(new Tuple3<>(sex, timestamp, aggregatedValue));
            }
            return results;
        }

        return Collections.emptyList();
    }

    public List<Tuple3<String, Timestamp, Double>> handleTimeout() {
        List<Tuple3<String, Timestamp, Double>> results = new ArrayList<>();
        while (!bufferedTimestamps.isEmpty()) {
            GetEarliestAggregatedValue earliestAggregatedValue = new GetEarliestAggregatedValue().invoke();
            Timestamp timestamp = earliestAggregatedValue.getTimestamp();
            Double aggregatedValue = earliestAggregatedValue.getValue();

            results.add(new Tuple3<>(sex, timestamp, aggregatedValue));
        }
        return results;
    }

    @Data
    private class GetEarliestAggregatedValue {
        private Timestamp timestamp;
        private Double value;

        public GetEarliestAggregatedValue invoke() {
            timestamp = bufferedTimestamps.first();

            if (sex.equals("m")) {
                // latest aggregation
                value = Double.valueOf(latestValues.get(timestamp)._2);
            } else {
                // avg aggregation
                value = mean(averagePools.get(timestamp));
            }

            // clean up buffers
            latestValues.remove(timestamp);
            averagePools.remove(timestamp);
            bufferedTimestamps.remove(timestamp);
            return this;
        }
    }
}
