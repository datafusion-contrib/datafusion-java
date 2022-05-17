package org.apache.arrow.datafusion;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

final class JNILoader {

  private JNILoader() {}

  private static final AtomicBoolean loaded = new AtomicBoolean(false);

  private static final String libraryName = "datafusion_jni";
  private static final String ERROR_MSG =
      String.format(
          "Unsupported OS/arch, cannot find %s or load %s from system libraries. Please try building from source the jar or providing %s in your system.",
          getResourceName(), libraryName, libraryName);

  private enum OsName {
    Windows,
    Osx,
    Linux;
  }

  private static final Map<OsName, String> OS_NAME_STRING_MAP =
      Map.of(OsName.Linux, "so", OsName.Osx, "dylib", OsName.Windows, "dll");

  private static OsName getOsName() {
    String os = System.getProperty("os.name").toLowerCase().replace(' ', '_');
    if (os.contains("win")) {
      return OsName.Windows;
    } else if (os.startsWith("mac") || os.contains("os_x")) {
      return OsName.Osx;
    } else {
      return OsName.Linux;
    }
  }

  private static String getResourceName() {
    return String.format("lib%s.%s", libraryName, getExtension());
  }

  private static String getExtension() {
    return OS_NAME_STRING_MAP.get(getOsName());
  }

  static synchronized void load() {
    if (loaded.get()) {
      return;
    }
    String resourceName = getResourceName();
    InputStream is = JNILoader.class.getResourceAsStream(resourceName);
    if (is == null) {
      try {
        System.loadLibrary(libraryName);
        loaded.set(true);
        return;
      } catch (UnsatisfiedLinkError e) {
        UnsatisfiedLinkError err =
            new UnsatisfiedLinkError(String.format("%s\n%s", e.getMessage(), ERROR_MSG));
        err.setStackTrace(e.getStackTrace());
        throw err;
      }
    }
    final File tempFile = extractToTempFile(is);
    try {
      System.load(tempFile.getAbsolutePath());
    } catch (UnsatisfiedLinkError le1) {
      // fall-back to loading from the system library path
      try {
        System.loadLibrary(libraryName);
        loaded.set(true);
      } catch (UnsatisfiedLinkError le2) {
        // display error in case problem with loading from temp folder
        // and from system library path - concatenate both messages
        UnsatisfiedLinkError err =
            new UnsatisfiedLinkError(
                String.format("%s\n%s\n%s", le1.getMessage(), le2.getMessage(), ERROR_MSG));
        err.setStackTrace(le2.getStackTrace());
        throw err;
      }
    }
  }

  private static File extractToTempFile(InputStream is) {
    final File tempFile;
    try {
      tempFile = File.createTempFile(libraryName, "." + getExtension(), null);
      tempFile.deleteOnExit();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot create temporary files", e);
    }
    try (InputStream in = is;
        FileOutputStream out = new FileOutputStream(tempFile)) {
      byte[] buf = new byte[4096];
      while (true) {
        int read = in.read(buf);
        if (read == -1) {
          break;
        }
        out.write(buf, 0, read);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to extract lib file and write to temp file", e);
    }
    return tempFile;
  }
}
