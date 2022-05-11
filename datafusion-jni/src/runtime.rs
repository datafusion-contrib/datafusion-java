use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use std::time::Duration;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_TokioRuntime_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    if let Ok(runtime) = Runtime::new() {
        // println!("successfully created tokio runtime");
        Box::into_raw(Box::new(runtime)) as jlong
    } else {
        // TODO error handling
        -1
    }
}
#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_TokioRuntime_destroyTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let runtime = unsafe { Box::from_raw(pointer as *mut Runtime) };
    runtime.shutdown_timeout(Duration::from_millis(100));
    // println!("successfully shutdown tokio runtime");
}
