package org.apache.avro.io;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class JsonGenericRecordReader {
    private ObjectMapper mapper;

    public JsonGenericRecordReader() {
        this.mapper = new ObjectMapper();
    }

    public JsonGenericRecordReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public GenericData.Record read(byte[] data, Schema schema) throws IOException {
        return read(mapper.readValue(data, Map.class), schema);
    }

    public GenericData.Record read(Map<String,Object> json, Schema schema) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            Schema.Field field = schema.getField(entry.getKey());
            record.set(field, read(field, field.schema(), entry.getValue()));
        }
        return record.build();
    }

    @SuppressWarnings("unchecked")
    private Object read(Schema.Field field, Schema schema, Object value) {
        switch (schema.getType()) {
            case RECORD:  return read(ensureType(value, Map.class, field), schema);
            case ARRAY:   return readArray(field, schema, ensureType(value, List.class, field));
            case MAP:     return readMap(field, schema, ensureType(value, Map.class, field));
            case UNION:   return readUnion(field, schema, value);
            case INT:     return ensureType(value, Number.class, field).intValue();
            case LONG:    return ensureType(value, Number.class, field).longValue();
            case FLOAT:   return ensureType(value, Number.class, field).floatValue();
            case DOUBLE:  return ensureType(value, Number.class, field).doubleValue();
            case BOOLEAN: return ensureType(value, Boolean.class, field);
            case ENUM:
            case STRING:  return ensureType(value, String.class, field);
            case NULL:    return ensureNull(value, field);
            default: throw new AvroTypeException("Unsupported type: " + field.schema().getType());
        }
    }

    private Object readArray(Schema.Field field, Schema schema, List<Object> values) {
        List<Object> record = new ArrayList<Object>();
        for (Object o : values) {
            record.add(read(field, schema.getElementType(), o));
        }
        return record;
    }

    private Object readMap(Schema.Field field, Schema schema, Map<String, Object> map) {
        Map<String, Object> record = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            record.put(entry.getKey(), read(field, schema.getValueType(), entry.getValue()));
        }
        return record;
    }


    private Object readUnion(Schema.Field field, Schema schema, Object value) {
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
            try {
                return read(field, type, value);
            } catch (AvroTypeException ex) {
                continue;
            }
        }
        throw new AvroTypeException(format("Could not evaluate union, field %s is expected to be one of these:", field.name()));
    }

    @SuppressWarnings("unchecked")
    private <T> T ensureType(Object value, Class<T> type, Schema.Field field) {
        if (type.isInstance(value)) {
            return (T) value;
        }
        throw new AvroTypeException(format("Field %s is expected to be of %s type.", field.name(), type.getName()));
    }

    private Object ensureNull(Object o, Schema.Field field) {
        if (o != null) {
            throw new AvroTypeException(format("Field %s was expected to be null.", field.name()));
        }
        return null;
    }
}
