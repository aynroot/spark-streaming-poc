package io.relayr.analytics.people.states;

import io.relayr.analytics.people.entities.EnhancedPerson;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

import static com.google.common.math.DoubleMath.mean;

public class PeopleAggregationStateSingleTimestamp implements Serializable {
    private final int watermark;
    private final String sex;
    private final Timestamp timestamp;

    private List<Integer> averagePool = new ArrayList<>();
    private Tuple2<Timestamp, Integer> latestValue;

    public PeopleAggregationStateSingleTimestamp(String sex, Timestamp ts) {
        this.sex = sex;
        this.timestamp = ts;
        watermark = sex.equals("f") ? 30 : 20;   // watermark = nCoarseGrainStepsToAccumulate * coarseGrainStep
    }

    public long update(EnhancedPerson person) {
        if (latestValue == null || !person.getTimestamp().before(latestValue._1)) {
            latestValue = new Tuple2<>(person.getTimestamp(), person.getAge());
        }
        averagePool.add(person.getAge());

        return watermark * 1000;
    }

    public List<Tuple3<String, Timestamp, Double>> getAggregatedValue() {
        List<Tuple3<String, Timestamp, Double>> results = new ArrayList<>();

        if (latestValue != null) {
            Double aggregatedValue;
            if (sex.equals("m")) {
                // latest aggregation
                aggregatedValue = Double.valueOf(latestValue._2);
            } else {
                // avg aggregation
                aggregatedValue = mean(averagePool);
            }
            results.add(new Tuple3<>(sex, timestamp, aggregatedValue));
        }

        return results;
    }
}
