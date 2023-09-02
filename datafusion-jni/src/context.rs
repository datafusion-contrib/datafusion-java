use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;
use tokio::runtime::Runtime;

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
        .expect("Couldn't get name as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let register_result = context
            .register_csv(&name, &path, CsvReadOptions::new())
            .await;
        let err_message = match register_result {
            Ok(_) => "".to_string(),
            Err(err) => err.to_string(),
        };
        let err_message = env
            .new_string(err_message)
            .expect("Couldn't create java string!");
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[(&err_message).into()],
        )
        .expect("failed to callback method");
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
        let err_message = match register_result {
            Ok(_) => "".to_string(),
            Err(err) => err.to_string(),
        };
        let err_message = env
            .new_string(err_message)
            .expect("Couldn't create java string!");
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[(&err_message).into()],
        )
        .expect("failed to callback method");
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
        match query_result {
            Ok(v) => {
                let empty_str = env
                    .new_string("".to_string())
                    .expect("Couldn't create java string!");
                let dataframe = Box::into_raw(Box::new(v)) as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[(&empty_str).into(), dataframe.into()],
                )
            }
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                let dataframe = -1 as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[(&err_message).into(), dataframe.into()],
                )
            }
        }
        .expect("failed to call method");
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
