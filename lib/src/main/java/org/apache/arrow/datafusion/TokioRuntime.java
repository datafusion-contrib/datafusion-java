package org.apache.arrow.datafusion;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class TokioRuntime implements Runtime {

  TokioRuntime(long pointer) {
    logger.printf(Level.INFO, "obtaining %x", pointer);
    this.pointer = pointer;
  }

  static native long createTokioRuntime();

  static native void destroyTokioRuntime(long pointer);

  private static final Logger logger = LogManager.getLogger(TokioRuntime.class);

  private final long pointer;

  @Override
  public void close() throws Exception {
    logger.printf(Level.INFO, "closing %x", pointer);
    destroyTokioRuntime(pointer);
  }
}
