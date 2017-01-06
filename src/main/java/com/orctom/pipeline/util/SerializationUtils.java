package com.orctom.pipeline.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public abstract class SerializationUtils {

  private static Kryo kryo = new Kryo();

  public static byte[] toBytes(Object obj) {
    Output output = new Output(1024, 8096);
    try {
      kryo.writeObject(output, obj);
      return output.toBytes();
    } finally {
      output.flush();
    }
  }

  public static <T> T toObject(byte[] bytes, Class<T> clazz) {
    Input input = new Input(bytes);
    return kryo.readObject(input, clazz);
  }
}
