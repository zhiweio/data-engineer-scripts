# pyspark-recipe

pyspark-recipe aims to provide a collection of reusable code snippets and functions that simplify common tasks in data
processing using Apache Spark.

### octet_split

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PySpark Recipe").getOrCreate()
df = spark.createDataFrame([("你好，世界。我是一个 UDF。",)], ["text"])

df.withColumn("split_text", octet_split_udf(df.text, size=F.lit(5))).show()

df.withColumn("padded_split_text", octet_pad_split_udf(df.text, width=F.lit(5), size=F.lit(5))).show()
```
