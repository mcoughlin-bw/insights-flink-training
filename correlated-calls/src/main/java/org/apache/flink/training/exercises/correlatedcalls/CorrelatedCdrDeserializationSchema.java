package org.apache.flink.training.exercises.correlatedcalls;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;

public class CorrelatedCdrDeserializationSchema extends AbstractDeserializationSchema<CorrelatedCdr> {

    private transient JsonMapper jsonMapper;

    @Override
    public CorrelatedCdr deserialize(byte[] message) throws IOException {
        return jsonMapper.readValue(message, CorrelatedCdr.class);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        jsonMapper = JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .propertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
                .build();
    }
}
