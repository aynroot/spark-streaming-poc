package io.relayr.analytics.vectors.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnhancedDeviceReading implements Serializable {
    private DeviceReading reading;
    private String vectorId;

    private Timestamp receivedTimestamp;
    private Timestamp coarseGrainedTimestamp;
}
