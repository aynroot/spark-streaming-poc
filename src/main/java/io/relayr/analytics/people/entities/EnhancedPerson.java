package io.relayr.analytics.people.entities;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;


@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class EnhancedPerson extends Person {
    private Timestamp coarseGrainedTimestamp;
    private Timestamp receivedTimestamp;

    private Integer coarseGrainStep;
    private String aggregationType;

    public EnhancedPerson(Person rawPerson) {
        this.timestamp = rawPerson.timestamp;
        this.age = rawPerson.age;
        this.name = rawPerson.name;
        this.sex = rawPerson.sex;
    }
}
