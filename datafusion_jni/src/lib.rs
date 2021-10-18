use arrow::ipc::writer::FileWriter;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::prelude::*;
use jni::objects::{GlobalRef, JClass, JObject, JString, JValue};
use jni::sys::{jbyteArray, jint, jlong, jstring};
use jni::JNIEnv;
use std::convert::Into;
use std::io::BufWriter;
use std::io::Cursor;
use std::sync::Arc;
use std::{sync::mpsc, thread, time::Duration};
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_TokioRuntime_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    if let Ok(runtime) = Runtime::new() {
        println!("successfully created tokio runtime");
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
    println!("successfully shutdown tokio runtime");
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultExecutionContext_registerParquet(
    env: JNIEnv,
    _class: JClass,
    _runtime: jlong,
    pointer: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let name: String = env
        .get_string(name)
        .expect("Couldn't get name as string!")
        .into();
    let path: String = env
        .get_string(path)
        .expect("Couldn't get name as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut ExecutionContext) };
    let register_result = context.register_parquet(&name, &path);
    let err_message: JValue = match register_result {
        Ok(_) => JValue::Void,
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string!");
            err_message.into()
        }
    };
    env.call_method(callback, "accept", "(Ljava/lang/Object;)V", &[err_message])
        .expect("failed to callback method");
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultExecutionContext_registerCsv(
    env: JNIEnv,
    _class: JClass,
    _runtime: jlong,
    pointer: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let name: String = env
        .get_string(name)
        .expect("Couldn't get name as string!")
        .into();
    let path: String = env
        .get_string(path)
        .expect("Couldn't get name as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut ExecutionContext) };
    let register_result = context.register_csv(&name, &path, CsvReadOptions::new());
    let err_message: JValue = match register_result {
        Ok(_) => JValue::Void,
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string!");
            err_message.into()
        }
    };
    env.call_method(callback, "accept", "(Ljava/lang/Object;)V", &[err_message])
        .expect("failed to callback method");
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultExecutionContext_querySql(
    env: JNIEnv,
    _class: JClass,
    _runtime: jlong,
    pointer: jlong,
    sql: JString,
    callback: JObject,
) {
    let sql: String = env
        .get_string(sql)
        .expect("Couldn't get sql as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut ExecutionContext) };
    let query_result = context.sql(&sql);
    match query_result {
        Ok(v) => {
            let dataframe = Box::into_raw(Box::new(v)) as jlong;
            env.call_method(
                callback,
                "callback",
                "(Ljava/lang/String;J)V",
                &[JValue::Void, dataframe.into()],
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
                &[err_message.into(), dataframe.into()],
            )
        }
    }
    .expect("failed to call method");
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ExecutionContexts_destroyExecutionContext(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut ExecutionContext) };
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ExecutionContexts_createExecutionContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let context = ExecutionContext::new();
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_collectDataframe(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<dyn DataFrame>) };
    let schema = dataframe.schema().into();
    runtime.block_on(async {
        let batches = dataframe
            .collect()
            .await
            .expect("failed to collect dataframe");
        let mut buff = Cursor::new(vec![0; 0]);
        {
            let mut writer = FileWriter::try_new(BufWriter::new(&mut buff), &schema)
                .expect("failed to create writer");
            for batch in batches {
                writer.write(&batch).expect("failed to write batch");
            }
            writer.finish().expect("failed to finish");
        }
        let err_message = env
            .new_string("".to_string())
            .expect("Couldn't create java string!");
        let ba = env
            .byte_array_from_slice(&buff.get_ref())
            .expect("cannot create empty byte array");
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;Ljava/lang/Object;)V",
            &[err_message.into(), ba.into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_showDataframe(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<dyn DataFrame>) };
    runtime.block_on(async {
        // TODO this is only added in Datafusion 6.0
        // dataframe.show().await;
        let err_message = env
            .new_string("".to_string())
            .expect("Couldn't create java string!");
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[err_message.into()],
        )
        .unwrap();
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_destroyDataFrame(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<dyn DataFrame>) };
}
