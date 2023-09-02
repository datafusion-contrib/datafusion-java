use datafusion::datasource::TableProvider;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_TableProviders_destroyTableProvider(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<dyn TableProvider>) };
}
