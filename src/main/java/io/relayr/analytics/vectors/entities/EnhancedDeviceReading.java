package io.relayr.analytics.vectors.entities;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class EnhancedDeviceReading implements Serializable {
    private DeviceReading reading;
    private String vectorId;

    private Timestamp receivedTimestamp;
    private Timestamp coarseGrainedTimestamp;
}
