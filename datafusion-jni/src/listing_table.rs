use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::TableProvider;
use jni::objects::{JClass, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;

use crate::util::set_object_result;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingTable_create(
    mut env: JNIEnv,
    _class: JClass,
    table_config: jlong,
    object_result: JObject,
) {
    let table_config = unsafe { &*(table_config as *const ListingTableConfig) };
    // Clone table config as it will be moved into ListingTable
    let table_config = ListingTableConfig {
        table_paths: table_config.table_paths.clone(),
        file_schema: table_config.file_schema.clone(),
        options: table_config.options.clone(),
    };
    let table_provider_result = ListingTable::try_new(table_config).map(|listing_table| {
        // Return as an Arc<dyn TableProvider> rather than ListingTable so this
        // can be passed into SessionContext.registerTable
        let table_provider: Arc<dyn TableProvider> = Arc::new(listing_table);
        Box::into_raw(Box::new(table_provider))
    });
    set_object_result(&mut env, object_result, table_provider_result);
}
