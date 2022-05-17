package org.apache.arrow.datafusion;

public class SessionContexts {

  private SessionContexts() {}

  static native long createSessionContext();

  static native void destroySessionContext(long pointer);

  static {
    JNILoader.load();
  }

  public static SessionContext create() {
    long pointer = createSessionContext();
    return new DefaultSessionContext(pointer);
  }
}
