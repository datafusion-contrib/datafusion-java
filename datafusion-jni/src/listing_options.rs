use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingOptions_create(
    mut env: JNIEnv,
    _class: JClass,
    format: jlong,
    file_extension: JString,
    collect_stat: jboolean,
) -> jlong {
    let format = unsafe { &*(format as *const Arc<dyn FileFormat>) };

    let file_extension: String = env
        .get_string(&file_extension)
        .expect("Couldn't get Java file_extension string")
        .into();

    let listing_options = ListingOptions::new(format.clone())
        .with_file_extension(file_extension)
        .with_collect_stat(collect_stat == 1u8);
    Box::into_raw(Box::new(listing_options)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingOptions_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut ListingOptions) };
}
