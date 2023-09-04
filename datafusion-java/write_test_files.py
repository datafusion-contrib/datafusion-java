import pyarrow as pa
import pyarrow.parquet as pq


num_rows = 100

dict_array_x = pa.DictionaryArray.from_arrays(
    pa.array([i % 3 for i in range(num_rows)]),
    pa.array(["one", "two", "three"])
)

dict_array_y = pa.DictionaryArray.from_arrays(
    pa.array([i % 3 for i in range(num_rows)]),
    pa.array(["four", "five", "six"])
)

table = pa.Table.from_arrays([dict_array_x, dict_array_y], ["x", "y"])
pq.write_table(table, "src/test/resources/dictionary_data.parquet")
