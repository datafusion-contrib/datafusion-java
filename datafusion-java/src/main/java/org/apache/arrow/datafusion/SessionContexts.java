package org.apache.arrow.datafusion;

import java.util.function.Consumer;

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
   * Create a new session context using a SessionConfig
   *
   * @param configPointer pointer to the native session config object to use
   * @return native pointer to the created session context
   */
  static native long createSessionContextWithConfig(long configPointer);

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

  /**
   * Create a new session context using the provided configuration
   *
   * @param config the configuration for the session
   * @return The created context
   */
  public static SessionContext withConfig(SessionConfig config) {
    long pointer = createSessionContextWithConfig(config.getPointer());
    return new DefaultSessionContext(pointer);
  }

  /**
   * Create a new session context using the provided callback to configure the session
   *
   * @param configuration callback to modify the {@link SessionConfig} for the session
   * @return The created context
   * @throws Exception if an error is encountered closing the session config resource
   */
  public static SessionContext withConfig(Consumer<SessionConfig> configuration) throws Exception {
    try (SessionConfig config = new SessionConfig().withConfiguration(configuration)) {
      long pointer = createSessionContextWithConfig(config.getPointer());
      return new DefaultSessionContext(pointer);
    }
  }
}
