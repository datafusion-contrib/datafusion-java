package org.apache.arrow.datafusion;

/** An execution context holds resources and is the entrance for obtaining {@link DataFrame} */
public interface ExecutionContext extends AutoCloseable, NativeProxy {

  /** Obtain the {@link DataFrame} by running the {@code sql} against the datafusion library */
  DataFrame sql(String sql);

  /** Get the runtime associated with this context */
  Runtime getRuntime();
}
