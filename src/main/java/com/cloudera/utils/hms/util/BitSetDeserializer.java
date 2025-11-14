package com.cloudera.utils.hms.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.BitSet;

// Deserializer
public class BitSetDeserializer extends JsonDeserializer<BitSet> {
    @Override
    public BitSet deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {
        BitSet bitSet = new BitSet();
        JsonNode node = p.getCodec().readTree(p);
        if (node.isArray()) {
            for (JsonNode element : node) {
                bitSet.set(element.asInt());
            }
        }
        return bitSet;
    }
}
