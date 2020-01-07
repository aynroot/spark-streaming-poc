package io.relayr.analytics.vectors.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;

@Data
@AllArgsConstructor
public class Vector implements Serializable {
    private Timestamp coarseGrainedTimestamp;
    private HashMap<String, Double> values;
}
