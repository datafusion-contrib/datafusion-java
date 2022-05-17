package org.apache.arrow.datafusion;

public class ExecutionContexts {

  private ExecutionContexts() {}

  static native long createExecutionContext();

  static native void destroyExecutionContext(long pointer);

  static {
    JNILoader.load();
  }

  public static ExecutionContext create() {
    long pointer = createExecutionContext();
    return new DefaultExecutionContext(pointer);
  }
}
