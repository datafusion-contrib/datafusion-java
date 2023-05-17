use futures::stream::TryStreamExt;
use arrow::ipc::writer::FileWriter;
use jni::objects::{JClass, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use std::convert::Into;
use std::io::BufWriter;
use std::io::Cursor;
use datafusion::physical_plan::SendableRecordBatchStream;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultRecordBatchStream_next(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    stream: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        let next = stream.try_next().await;
        match next {
            Ok(Some(batch)) => {
                let schema = batch.schema();
                let mut buff = Cursor::new(vec![0; 0]);
                {
                    let mut writer = FileWriter::try_new(BufWriter::new(&mut buff), &schema)
                        .expect("failed to create writer");
                    writer.write(&batch).expect("failed to write batch");
                    writer.finish().expect("failed to finish writer");
                }
                let err_message = env
                    .new_string("")
                    .expect("Couldn't create java string!");
                let ba = env
                    .byte_array_from_slice(&buff.get_ref())
                    .expect("cannot create byte array");
                env.call_method(
                    callback,
                    "accept",
                    "(Ljava/lang/Object;Ljava/lang/Object;)V",
                    &[err_message.into(), ba.into()],
                )
            }
            Ok(None) => {
                let err_message = env
                    .new_string("")
                    .expect("Couldn't create java string!");
                let buff = Cursor::new(vec![0; 0]);
                let ba = env
                    .byte_array_from_slice(&buff.get_ref())
                    .expect("cannot create empty byte array");
                env.call_method(
                    callback,
                    "accept",
                    "(Ljava/lang/Object;Ljava/lang/Object;)V",
                    &[err_message.into(), ba.into()],
                )
            }
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                let buff = Cursor::new(vec![0; 0]);
                let ba = env
                    .byte_array_from_slice(&buff.get_ref())
                    .expect("cannot create empty byte array");
                env.call_method(
                    callback,
                    "accept",
                    "(Ljava/lang/Object;Ljava/lang/Object;)V",
                    &[err_message.into(), ba.into()],
                )
            }
        }
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultRecordBatchStream_getSchema(
    env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let mut buff = Cursor::new(vec![0; 0]);
    {
        let mut writer = FileWriter::try_new(BufWriter::new(&mut buff), &schema)
            .expect("failed to create writer");
        writer.finish().expect("failed to finish writer");
    }
    let err_message = env
        .new_string("")
        .expect("Couldn't create java string!");
    let ba = env
        .byte_array_from_slice(&buff.get_ref())
        .expect("cannot create byte array");
    env.call_method(
        callback,
        "accept",
        "(Ljava/lang/Object;Ljava/lang/Object;)V",
        &[err_message.into(), ba.into()],
    )
    .expect("failed to call method");
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultRecordBatchStream_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SendableRecordBatchStream) };
}
