use arrow::ipc::writer::FileWriter;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use std::convert::Into;
use std::io::BufWriter;
use std::io::Cursor;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_collectDataframe(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<DataFrame>) };
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
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<DataFrame>) };
    runtime.block_on(async {
        let r = dataframe.show().await;
        let err_message: JValue = match r {
            Ok(_) => JValue::Void,
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                err_message.into()
            }
        };
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[err_message.into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_writeParquet(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<DataFrame>) };
    let path: String = env
        .get_string(path)
        .expect("Couldn't get path as string!")
        .into();
    runtime.block_on(async {
        let r = dataframe.write_parquet(&path, None).await;
        let err_message: JValue = match r {
            Ok(_) => JValue::Void,
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                err_message.into()
            }
        };
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[err_message.into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_writeCsv(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<DataFrame>) };
    let path: String = env
        .get_string(path)
        .expect("Couldn't get path as string!")
        .into();
    runtime.block_on(async {
        let r = dataframe.write_csv(&path).await;
        let err_message: JValue = match r {
            Ok(_) => JValue::Void,
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                err_message.into()
            }
        };
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[err_message.into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_registerTable(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    session: jlong,
    name: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &mut *(dataframe as *mut Arc<DataFrame>) };
    let context = unsafe { &mut *(session as *mut SessionContext) };
    let name: String = env
        .get_string(name)
        .expect("Couldn't get name as string!")
        .into();
    runtime.block_on(async {
        let r = context.register_table(name.as_str(), dataframe.clone());
        let err_message: JValue = match r {
            Ok(_) => JValue::Void,
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                err_message.into()
            }
        };
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;)V",
            &[err_message.into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_destroyDataFrame(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<DataFrame>) };
}
