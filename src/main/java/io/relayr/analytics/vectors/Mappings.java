package io.relayr.analytics.vectors;

import java.util.HashMap;

public class Mappings {
    public static final HashMap<String, String> deviceToVectors = new HashMap<>();
    public static final HashMap<String, Integer> vectorsWatermarks = new HashMap<>();
    public static final HashMap<String, String> vectorsFeatures = new HashMap<>();
    public static final HashMap<String, Integer> featuresCoarseGrainSteps = new HashMap<>();
    public static final HashMap<String, String> featuresAggregationTypes = new HashMap<>();

    static {
        /*
        d0 (f1, 5sec, average)  \
                                  - v1
        d1 (f2, 10sec, average) /


        d2 (f3, f4, 5sec latest both) - v2, v3
         */

        deviceToVectors.put("d0", "v1");
        deviceToVectors.put("d1", "v1");
        deviceToVectors.put("d2", "v2,v3");

        vectorsWatermarks.put("v1", 10);
        vectorsWatermarks.put("v2", 20);
        vectorsWatermarks.put("v3", 30);

        vectorsFeatures.put("v1", "f1,f2");
        vectorsFeatures.put("v2", "f3");
        vectorsFeatures.put("v3", "f4");

        featuresCoarseGrainSteps.put("f1", 5000);
        featuresCoarseGrainSteps.put("f2", 10000);
        featuresCoarseGrainSteps.put("f3", 5000);
        featuresCoarseGrainSteps.put("f4", 5000);

        featuresAggregationTypes.put("f1", "average");
        featuresAggregationTypes.put("f2", "average");
        featuresAggregationTypes.put("f3", "latest");
        featuresAggregationTypes.put("f4", "latest");
    }
}
