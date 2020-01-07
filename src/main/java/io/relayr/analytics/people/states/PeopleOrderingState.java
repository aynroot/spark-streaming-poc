package io.relayr.analytics.people.states;

import java.io.Serializable;

public class PeopleOrderingState implements Serializable {
    private int lastIndex = 0;

    public PeopleOrderingState() {
    }

    public int getNextIndex() {
        return lastIndex++;
    }
}
