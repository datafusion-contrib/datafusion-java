package org.apache.arrow.datafusion;

public interface ExecutionContext extends AutoCloseable, NativeProxy {

  DataFrame sql(String sql);

  Runtime getRuntime();
}
