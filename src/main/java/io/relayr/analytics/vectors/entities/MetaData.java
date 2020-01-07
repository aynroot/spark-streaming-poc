package io.relayr.analytics.vectors.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class MetaData implements Serializable {
    private String vectorId;
}
