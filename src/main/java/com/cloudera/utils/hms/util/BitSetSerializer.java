package com.cloudera.utils.hms.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.BitSet;

// Serializer
public class BitSetSerializer extends JsonSerializer<BitSet> {
    @Override
    public void serialize(BitSet value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
        gen.writeStartArray();
        for (int i = value.nextSetBit(0); i >= 0; i = value.nextSetBit(i + 1)) {
            gen.writeNumber(i);
        }
        gen.writeEndArray();
    }
}

