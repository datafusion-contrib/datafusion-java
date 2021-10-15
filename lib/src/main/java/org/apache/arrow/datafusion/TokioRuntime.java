package org.apache.arrow.datafusion;

final class TokioRuntime extends AbstractProxy implements Runtime {

  TokioRuntime(long pointer) {
    super(pointer);
  }

  @Override
  void doClose(long pointer) {
    destroyTokioRuntime(pointer);
  }

  static TokioRuntime create() {
    long pointer = TokioRuntime.createTokioRuntime();
    if (pointer <= 0) {
      throw new IllegalStateException("failed to create runtime");
    }
    return new TokioRuntime(pointer);
  }

  static native long createTokioRuntime();

  static native void destroyTokioRuntime(long pointer);
}
