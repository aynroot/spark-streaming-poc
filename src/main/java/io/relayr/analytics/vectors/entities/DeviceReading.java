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
public class DeviceReading implements Serializable {
    private String device;
    private String name;
    private Double value;
    private Timestamp timestamp;

    private Long kafkaTimestamp;
}
