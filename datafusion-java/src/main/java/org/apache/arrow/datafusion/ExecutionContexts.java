package org.apache.arrow.datafusion;

public class ExecutionContexts {

  private ExecutionContexts() {}

  static native long createExecutionContext();

  static native void destroyExecutionContext(long pointer);

  static {
    JNILoader.load();
  }

  public static SessionContext create() {
    long pointer = createExecutionContext();
    return new DefaultSessionContext(pointer);
  }
}
