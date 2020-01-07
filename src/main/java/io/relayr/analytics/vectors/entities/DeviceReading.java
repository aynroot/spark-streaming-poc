package io.relayr.analytics.vectors.entities;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class DeviceReading implements Serializable {
    private String device;
    private String name;
    private Double value;
    private Timestamp timestamp;

    private Long kafkaTimestamp;
}
