package org.apache.arrow.datafusion;

/** Manages session contexts */
public class SessionContexts {

  private SessionContexts() {}

  /**
   * Create a new session context
   *
   * @return native pointer to the created session context
   */
  static native long createSessionContext();

  /**
   * Destroy a session context
   *
   * @param pointer native pointer to the session context to destroy
   */
  static native void destroySessionContext(long pointer);

  static {
    JNILoader.load();
  }

  /**
   * Create a new default session context
   *
   * @return The created context
   */
  public static SessionContext create() {
    long pointer = createSessionContext();
    return new DefaultSessionContext(pointer);
  }
}
