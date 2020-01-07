package io.relayr.analytics.people.entities;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Person implements Serializable {
    protected String name;
    protected String sex;
    protected int age;

    protected Timestamp timestamp;
}
