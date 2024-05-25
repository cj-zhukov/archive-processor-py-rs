import archive_processor_py_rs
import datafusion
import pyarrow as pa
import pandas as pd
from io import BytesIO
from PIL import Image
import imagehash


def calc_image_phash(bin_img):
    pil_img = Image.open(BytesIO(bin_img))
    phash = imagehash.phash(pil_img, hash_size=16)
    return str(phash)


def struct_example():
    rows = archive_processor_py_rs.run_txt("./data/foo_txt.zip")
    (pkeys, file_names, content_all) = ([], [], [])
    for row in rows:
        pkeys.append(row.pkey)
        file_names.append(row.file_name)
        content_all.append(row.content)

    batch = pa.RecordBatch.from_arrays(
        [pa.array(pkeys), pa.array(file_names), pa.array(content_all)],
        names=["pkey", "file_name", "content"],
    )
    ctx = datafusion.SessionContext()
    df = ctx.create_dataframe([[batch]])
    print(df)
    # df_pd = df.to_pandas()
    # df_pl = df.to_polars()


def batches_example():
    batches = archive_processor_py_rs.run_img("./data/foo_jpg.zip")
    ctx = datafusion.SessionContext()
    ctx.register_record_batches("t", [batches])
    df = ctx.sql("select * from t")
    image_df = df.to_arrow_table()
    data_vals = image_df.column("data_val").to_pylist()
    phash = [calc_image_phash(data_val) for data_val in data_vals]
    df = image_df.add_column(1, 'phash', [phash])
    df = ctx.from_arrow_table(df)
    df = df.to_polars()
    print(df)



if __name__ == "__main__":
    struct_example()
    batches_example()