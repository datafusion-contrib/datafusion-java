use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::util::{set_error_message, set_object_result};

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_registerCsv(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    pointer: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let name: String = env
        .get_string(&name)
        .expect("Couldn't get name as string!")
        .into();
    let path: String = env
        .get_string(&path)
        .expect("Couldn't get path as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let register_result = context
            .register_csv(&name, &path, CsvReadOptions::new())
            .await;
        set_error_message(&mut env, callback, register_result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_registerTable(
    mut env: JNIEnv,
    _class: JClass,
    pointer: jlong,
    name: JString,
    table_provider: jlong,
) -> jlong {
    let name: String = env
        .get_string(&name)
        .expect("Couldn't get name as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    let table_provider = unsafe { &*(table_provider as *const Arc<dyn TableProvider>) };
    let result = context.register_table(&name, table_provider.clone());
    match result {
        // TODO this is to be fixed on datafusion side as duplicates will not be returned
        // and instead returned as err
        Ok(Some(v)) => Box::into_raw(Box::new(v)) as jlong,
        Ok(None) => 0,
        Err(err) => {
            env.throw_new("java/lang/Exception", err.to_string())
                .unwrap();
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_registerParquet(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    pointer: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let name: String = env
        .get_string(&name)
        .expect("Couldn't get name as string!")
        .into();
    let path: String = env
        .get_string(&path)
        .expect("Couldn't get path as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let register_result = context
            .register_parquet(&name, &path, ParquetReadOptions::default())
            .await;
        set_error_message(&mut env, callback, register_result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_querySql(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    pointer: jlong,
    sql: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let sql: String = env
        .get_string(&sql)
        .expect("Couldn't get sql as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let query_result = context.sql(&sql).await;
        set_object_result(
            &mut env,
            callback,
            query_result.map(|df| Box::into_raw(Box::new(df))),
        );
    });
}
#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionContexts_destroySessionContext(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionContext) };
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionContexts_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let context = SessionContext::new();
    Box::into_raw(Box::new(context)) as jlong
}
