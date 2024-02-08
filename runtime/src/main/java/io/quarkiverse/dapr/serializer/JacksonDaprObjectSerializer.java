package io.quarkiverse.dapr.serializer;

import java.io.IOException;
import java.lang.reflect.Method;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.MessageLite;

import io.dapr.client.domain.CloudEvent;
import io.dapr.serializer.DaprObjectSerializer;
import io.dapr.utils.TypeRef;

/**
 * JacksonDaprObjectSerializer
 *
 * @author naah69
 * @date 2022/4/19 12:52 PM
 */
public class JacksonDaprObjectSerializer implements DaprObjectSerializer {

    private final ObjectMapper objectMapper;

    public JacksonDaprObjectSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(Object state) throws IOException {
        if (state == null) {
            return null;
        }

        if (state.getClass() == Void.class) {
            return null;
        }

        // Have this check here to be consistent with deserialization (see deserialize()
        // method below).
        if (state instanceof byte[]) {
            return (byte[]) state;
        }

        // Proto buffer class is serialized directly.
        if (state instanceof MessageLite) {
            return ((MessageLite) state).toByteArray();
        }

        // Not string, not primitive, so it is a complex type: we use JSON for that.
        return this.objectMapper.writeValueAsBytes(state);
    }

    @Override
    public <T> T deserialize(byte[] data, TypeRef<T> type) throws IOException {
        return this.deserialize(data, this.objectMapper.constructType(type.getType()));
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(byte[] content, JavaType javaType) throws IOException {
        if ((javaType == null) || javaType.isTypeOrSubTypeOf(Void.class)) {
            return null;
        }

        if (javaType.isPrimitive()) {
            return this.deserializePrimitives(content, javaType);
        }

        if (content == null) {
            return null;
        }

        // Deserialization of GRPC response fails without this check since it does not
        // come as base64 encoded byte[].
        if (javaType.hasRawClass(byte[].class)) {
            return (T) content;
        }

        if (content.length == 0) {
            return null;
        }

        if (javaType.hasRawClass(CloudEvent.class)) {
            return (T) CloudEvent.deserialize(content);
        }

        if (javaType.isTypeOrSubTypeOf(MessageLite.class)) {
            try {
                Method method = javaType.getRawClass().getDeclaredMethod("parseFrom", byte[].class);
                if (method != null) {
                    return (T) method.invoke(null, content);
                }
            } catch (NoSuchMethodException e) {
                // It was a best effort. Skip this try.
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return this.objectMapper.readValue(content, javaType);
    }

    /**
     * Parses a given String to the corresponding object defined by class.
     *
     * @param content Value to be parsed.
     * @param javaType Type of the expected result type.
     * @param <T> Result type.
     * @return Result as corresponding type.
     * @throws IOException if cannot deserialize primitive time.
     */
    @SuppressWarnings("unchecked")
    private <T> T deserializePrimitives(byte[] content, JavaType javaType) throws IOException {
        if ((content == null) || (content.length == 0)) {
            if (javaType.hasRawClass(boolean.class)) {
                return (T) Boolean.FALSE;
            }

            if (javaType.hasRawClass(byte.class)) {
                return (T) Byte.valueOf((byte) 0);
            }

            if (javaType.hasRawClass(short.class)) {
                return (T) Short.valueOf((short) 0);
            }

            if (javaType.hasRawClass(int.class)) {
                return (T) Integer.valueOf(0);
            }

            if (javaType.hasRawClass(long.class)) {
                return (T) Long.valueOf(0L);
            }

            if (javaType.hasRawClass(float.class)) {
                return (T) Float.valueOf(0);
            }

            if (javaType.hasRawClass(double.class)) {
                return (T) Double.valueOf(0);
            }

            if (javaType.hasRawClass(char.class)) {
                return (T) Character.valueOf(Character.MIN_VALUE);
            }

            return null;
        }

        return this.objectMapper.readValue(content, javaType);
    }

    @Override
    public String getContentType() {
        return "application/json";
    }
}
