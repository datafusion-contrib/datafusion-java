use datafusion::execution::context::SessionConfig;
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_create(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let session_config = Box::new(SessionConfig::new());
    Box::into_raw(session_config) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionConfig) };
}

// Helper macros to implement boolean options

macro_rules! bool_getter {
    ($name:ident, $($property_path:ident).+) => {
        #[no_mangle]
        pub extern "system" fn $name(
            _env: JNIEnv,
            _class: JClass,
            pointer: jlong,
        ) -> jboolean {
            let config = unsafe { &*(pointer as *const SessionConfig) };
            let property_value = config.options().$($property_path).+;
            if property_value {
                1u8
            } else {
                0u8
            }
        }
    }
}

macro_rules! bool_setter {
    ($name:ident, $($property_path:ident).+) => {
        #[no_mangle]
        pub extern "system" fn $name(
            _env: JNIEnv,
            _class: JClass,
            pointer: jlong,
            enabled: jboolean,
        ) {
            let config = unsafe { &mut *(pointer as *mut SessionConfig) };
            config.options_mut().$($property_path).+ = enabled != 0u8;
        }
    }
}

macro_rules! usize_getter {
    ($name:ident, $($property_path:ident).+) => {
        #[no_mangle]
        pub extern "system" fn $name(
            _env: JNIEnv,
            _class: JClass,
            pointer: jlong,
        ) -> jlong {
            let config = unsafe { &*(pointer as *const SessionConfig) };
            let property_value = config.options().$($property_path).+;
            property_value as jlong
        }
    }
}

macro_rules! usize_setter {
    ($name:ident, $($property_path:ident).+) => {
        #[no_mangle]
        pub extern "system" fn $name(
            _env: JNIEnv,
            _class: JClass,
            pointer: jlong,
            value: jlong,
        ) {
            let config = unsafe { &mut *(pointer as *mut SessionConfig) };
            config.options_mut().$($property_path).+ = value as usize;
        }
    }
}

// ExecutionOptions

usize_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getExecutionOptionsBatchSize,
    execution.batch_size
);
usize_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setExecutionOptionsBatchSize,
    execution.batch_size
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getExecutionOptionsCoalesceBatches,
    execution.coalesce_batches
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setExecutionOptionsCoalesceBatches,
    execution.coalesce_batches
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getExecutionOptionsCollectStatistics,
    execution.collect_statistics
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setExecutionOptionsCollectStatistics,
    execution.collect_statistics
);

usize_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getExecutionOptionsTargetPartitions,
    execution.target_partitions
);
usize_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setExecutionOptionsTargetPartitions,
    execution.target_partitions
);

// ParquetOptions

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsEnablePageIndex,
    execution.parquet.enable_page_index
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsEnablePageIndex,
    execution.parquet.enable_page_index
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsPruning,
    execution.parquet.pruning
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsPruning,
    execution.parquet.pruning
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsSkipMetadata,
    execution.parquet.skip_metadata
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsSkipMetadata,
    execution.parquet.skip_metadata
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsPushdownFilters,
    execution.parquet.pushdown_filters
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsPushdownFilters,
    execution.parquet.pushdown_filters
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsReorderFilters,
    execution.parquet.reorder_filters
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsReorderFilters,
    execution.parquet.reorder_filters
);

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsMetadataSizeHint(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) -> jlong {
    let config = unsafe { &*(pointer as *const SessionConfig) };
    let size_hint = config.options().execution.parquet.metadata_size_hint;
    match size_hint {
        Some(size_hint) => size_hint as jlong,
        None => -1 as jlong,
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsMetadataSizeHint(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
    value: jlong,
) {
    let config = unsafe { &mut *(pointer as *mut SessionConfig) };
    if value >= 0 {
        config.options_mut().execution.parquet.metadata_size_hint = Some(value as usize);
    } else {
        config.options_mut().execution.parquet.metadata_size_hint = None;
    }
}

// SqlParserOptions

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getSqlParserOptionsParseFloatAsDecimal,
    sql_parser.parse_float_as_decimal
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setSqlParserOptionsParseFloatAsDecimal,
    sql_parser.parse_float_as_decimal
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getSqlParserOptionsEnableIdentNormalization,
    sql_parser.enable_ident_normalization
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setSqlParserOptionsEnableIdentNormalization,
    sql_parser.enable_ident_normalization
);

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_getSqlParserOptionsDialect<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    pointer: jlong,
) -> JString<'local> {
    let config = unsafe { &*(pointer as *const SessionConfig) };
    let dialect = &config.options().sql_parser.dialect;
    env.new_string(dialect)
        .expect("Couldn't create Java string")
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_setSqlParserOptionsDialect(
    mut env: JNIEnv,
    _class: JClass,
    pointer: jlong,
    dialect: JString,
) {
    let config = unsafe { &mut *(pointer as *mut SessionConfig) };
    let dialect: String = env
        .get_string(&dialect)
        .expect("Couldn't get dialect string")
        .into();
    config.options_mut().sql_parser.dialect = dialect;
}
