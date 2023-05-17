package org.apache.arrow.datafusion;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** helper class that calls into native stack for {@link DataFrame} */
final class DataFrames {

  private DataFrames() {}

  static native void destroyDataFrame(long pointer);

  static native void showDataframe(long runtime, long dataframe, Consumer<String> callback);

  static native void collectDataframe(
      long runtime, long dataframe, BiConsumer<String, byte[]> callback);

  static native void executeStream(long runtime, long dataframe, ObjectResultCallback callback);

  static native void writeParquet(
      long runtime, long dataframe, String path, Consumer<String> callback);

  static native void writeCsv(long runtime, long dataframe, String path, Consumer<String> callback);

  static native void registerTable(
      long runtime, long dataframe, long context, String name, Consumer<String> callback);
}
