package org.apache.pinot.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

public class Roaring64BitmapUtils {
  private Roaring64BitmapUtils() {
  }

  public static byte[] serialize(ImmutableLongBitmapDataProvider bitmap) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      bitmap.serialize(dataOutputStream);
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while serializing Roaring64Bitmap", e);
    }
  }

  public static void serialize(ImmutableLongBitmapDataProvider bitmap, ByteBuffer into) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      bitmap.serialize(dataOutputStream);
      byte[] serializedData = byteArrayOutputStream.toByteArray();
      into.put(serializedData);
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while serializing Roaring64Bitmap", e);
    }
  }

  public static Roaring64Bitmap deserialize(byte[] bytes) {
    return deserialize(ByteBuffer.wrap(bytes));
  }

  public static Roaring64Bitmap deserialize(ByteBuffer byteBuffer) {
    Roaring64Bitmap bitmap = new Roaring64Bitmap();
    try {
      bitmap.deserialize(new DataInputStream(
          new ByteArrayInputStream(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(),
              byteBuffer.remaining())));
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while deserializing Roaring64Bitmap", e);
    }
    return bitmap;
  }
}
