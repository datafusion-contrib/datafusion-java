package org.apache.arrow.datafusion;

/** A runtime represents the underlying async runtime in datafusion engine */
public interface Runtime extends AutoCloseable, NativeProxy {}
