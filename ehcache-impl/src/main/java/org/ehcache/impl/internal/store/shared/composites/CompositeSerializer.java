package org.ehcache.impl.internal.store.shared.composites;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.nio.ByteBuffer;
import java.util.Map;

public class CompositeSerializer implements Serializer<CompositeValue<?>> {
  private final Map<Integer, Serializer<?>> serializerMap;

  public CompositeSerializer(Map<Integer, Serializer<?>> serializerMap) {
    this.serializerMap = serializerMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public ByteBuffer serialize(CompositeValue<?> compositeValue) throws SerializerException {
    int id = compositeValue.getStoreId();
    ByteBuffer valueForm = ((Serializer)serializerMap.get(id)).serialize(compositeValue.getValue());
    ByteBuffer compositeForm = ByteBuffer.allocate(4 + valueForm.remaining());
    compositeForm.putInt(compositeValue.getStoreId());
    compositeForm.put(valueForm);
    compositeForm.flip();
    return compositeForm;
  }

  @Override
  public CompositeValue<?> read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    int id = binary.getInt();
    Serializer<?> serializer = serializerMap.get(id);
    return new CompositeValue<>(id, serializer.read(binary));
  }

  @Override
  public boolean equals(CompositeValue<?> object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    return object.equals(read(binary));
  }
}
