package org.apache.arrow.datafusion;

public interface Runtime extends AutoCloseable {

  static Runtime create() {
    long pointer = TokioRuntime.createTokioRuntime();
    if (pointer <= 0) {
      throw new IllegalStateException("failed to create runtime");
    }
    return new TokioRuntime(pointer);
  }
}
