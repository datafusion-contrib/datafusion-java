use arrow::array::Array;
use arrow::array::StructArray;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::TryStreamExt;
use jni::objects::{JClass, JObject, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use std::convert::Into;
use std::ptr::addr_of_mut;
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
                // Convert to struct array for compatibility with FFI
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                let err_message = env.new_string("").expect("Couldn't create java string!");
                let array_address = addr_of_mut!(ffi_array) as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[err_message.into(), array_address.into()],
                )
            }
            Ok(None) => {
                let err_message = env.new_string("").expect("Couldn't create java string!");
                let array_address = 0 as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[err_message.into(), array_address.into()],
                )
            }
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                let array_address = -1 as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[err_message.into(), array_address.into()],
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
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            let schema_address = addr_of_mut!(ffi_schema) as jlong;
            env.call_method(
                callback,
                "callback",
                "(Ljava/lang/String;J)V",
                &[JValue::Void, schema_address.into()],
            )
        }
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string!");
            let schema_address = -1 as jlong;
            env.call_method(
                callback,
                "callback",
                "(Ljava/lang/String;J)V",
                &[err_message.into(), schema_address.into()],
            )
        }
    }
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
