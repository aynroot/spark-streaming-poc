package io.relayr.analytics.vectors.states;

import com.google.protobuf.InvalidProtocolBufferException;
import io.relayr.analytics.vectors.entities.DeviceReading;
import io.relayr.analytics.vectors.entities.EnhancedDeviceReading;
import org.junit.Test;

import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregationStateTest {

    @Test
    public void shouldSerializeAndDeserializeCorrectly() throws InvalidProtocolBufferException {
        String vectorId = "v1";
        AggregationState state = new AggregationState(vectorId);
        Timestamp ts1 = new Timestamp(1000);
        Timestamp ts2 = new Timestamp(2000);
        EnhancedDeviceReading reading1 = EnhancedDeviceReading.builder()
                .vectorId(vectorId).coarseGrainedTimestamp(ts1).receivedTimestamp(ts1)
                .reading(DeviceReading.builder().device("d1").name("f1").value(10.0).kafkaTimestamp(1L).timestamp(ts1).build()).build();
        EnhancedDeviceReading reading2 = EnhancedDeviceReading.builder()
                .vectorId(vectorId).coarseGrainedTimestamp(ts1).receivedTimestamp(ts1)
                .reading(DeviceReading.builder().device("d1").name("f2").value(20.0).kafkaTimestamp(1L).timestamp(ts1).build()).build();
        EnhancedDeviceReading reading3 = EnhancedDeviceReading.builder()
                .vectorId(vectorId).coarseGrainedTimestamp(ts2).receivedTimestamp(ts2)
                .reading(DeviceReading.builder().device("d1").name("f1").value(100.0).kafkaTimestamp(1L).timestamp(ts2).build()).build();
        EnhancedDeviceReading reading4 = EnhancedDeviceReading.builder()
                .vectorId(vectorId).coarseGrainedTimestamp(ts2).receivedTimestamp(ts2)
                .reading(DeviceReading.builder().device("d1").name("f2").value(200.0).kafkaTimestamp(1L).timestamp(ts2).build()).build();

        state.update(reading1);
        state.update(reading2);
        state.update(reading3);
        state.update(reading4);

        byte[] bytes = state.serialize();
        AggregationState deserializedState = AggregationState.deserialize(bytes);

        assertThat(deserializedState).isEqualToComparingFieldByFieldRecursively(state);
    }
}