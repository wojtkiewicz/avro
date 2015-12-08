package org.apache.avro.io;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonDatumReader {
    private ObjectMapper mapper = new ObjectMapper();

    public GenericData.Record read(byte[] data, Schema schema) throws IOException {
        Map<String,Object> json = mapper.readValue(data, new TypeReference<Map<String,Object>>() {});
        return read(json, schema);
    }

    public GenericData.Record read(Map<String,Object> json, Schema schema) {
        final GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            Schema.Field field = schema.getField(entry.getKey());
            Object value = read(field, field.schema(), entry.getValue());
            record.set(field, value);
        }
        return record.build();
    }

    private Object read(Schema.Field field, Schema schema, Object value) {
        switch (schema.getType()) {
            case RECORD:  return readRecord(schema, value);
            case ARRAY:   return readArray(field, schema, value);
            case MAP:     return readMap(field, schema, value);
            case UNION:   return readUnion(field, schema, value);
            case INT:     return ((Number)value).intValue();
            case LONG:    return ((Number)value).longValue();
            case FLOAT:   return ((Number)value).floatValue();
            case DOUBLE:  return ((Number)value).doubleValue();
            case STRING:
            case BOOLEAN:
            case ENUM:
            case NULL:    return value;
            default: throw new AvroRuntimeException("Unsupported type: " + field.schema().getType());
        }
    }

    @SuppressWarnings("unchecked")
    private Object readRecord(Schema schema, Object value) {
        return read((Map<String, Object>)value, schema);
    }

    @SuppressWarnings("unchecked")
    private Object readArray(Schema.Field field, Schema schema, Object value) {
        List<Object> values = (List<Object>) value;
        List<Object> record = new ArrayList<Object>();
        for (Object o : values) {
            record.add(read(field, schema.getElementType(), o));
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    private Object readMap(Schema.Field field, Schema schema, Object value) {
        Map<String, Object> values = (Map<String, Object>) value;
        Map<String, Object> record = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            record.put(entry.getKey(), read(field, schema.getValueType(), entry.getValue()));
        }
        return record;
    }

    private Object readUnion(Schema.Field field, Schema schema, Object value) {
        List<Schema> types = field.schema().getTypes();
        if ((value == null) && (Schema.Type.NULL == types.get(0).getType())) {
            return field.defaultVal();
        }
        return read(field, types.get(1), value);
    }
}
