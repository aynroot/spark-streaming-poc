package io.relayr.analytics.vectors.states;

import io.relayr.analytics.vectors.entities.Vector;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class AugmentationState implements Serializable {
    private HashMap<String, Double> latestValues = new HashMap<>();

    public Vector getAugmentedVector(Vector vector) {
        for (Map.Entry<String, Double> featureValue : vector.getValues().entrySet()) {
            if (featureValue.getValue() == null) {
                if (latestValues.containsKey(featureValue.getKey())) {
                    vector.getValues().put(featureValue.getKey(), latestValues.get(featureValue.getKey()));
                }
            } else {
                latestValues.put(featureValue.getKey(), featureValue.getValue());
            }
        }

        return vector;
    }
}
