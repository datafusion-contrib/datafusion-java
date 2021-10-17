package org.apache.arrow.datafusion;

/**
 * A native proxy is a proxy that points to a Rust managed object so that when it requires releasing
 * resources the point will be used.
 */
interface NativeProxy {

  long getPointer();
}
