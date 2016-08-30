# User Defined Functions (UDFs)

[User Defined Functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=udf#pyspark.sql.functions.udf) are powerful. They work on DataFrame column(s) to transform them. They are SQL-oriented functions, so they can work in `select` and other SQL expressions. They are also Python functions, so they can be called in `withColumn`.

## Using UDF with the wrong return data type

Spark is strict on the return data type of UDFs. The following code will fail:

```from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf


def get_an_int():
    return 1


dataframe.withColumn(
    'ones',
    udf(get_an_int, DoubleType())()
)```

Because under the hood Spark (or more precisely, Java/Scala) doesn't do the type conversion for you.

Note 

 1. The default return type of `udf` is `StringType`

 2. Most of the data types could accept `None`


## Filtering inside UDF

We can use `RDD.flatMap` to process data while filtering them, for example,

```def get_if_odd(x):
	if x % 2 == 1:
		yield x


data = sc.parallelize([
	1, 2, 3, 4
])

print data.flatMap(get_if_odd).collect()
```
The above code gives the output

> [1, 3]

But the same function won't work with `udf`. The following code will fail:

```
from pyspark.sql.functions import udf, col
from pyspark.sql.types import Row, IntegerType

data = sc.parallelize([
	Row(key=1, val=2),
	Row(key=2, val=2),
	Row(key=3, val=2),
	Row(key=4, val=2),
]).toDF()

print data.withColumn(
	'extracted_key',
	udf(get_if_odd, IntegerType())(col('key'))
).collect()```

With error message:

> PicklingError: Can't pickle <type 'generator'>: attribute lookup __builtin__.generator failed

In order to make it work, we need to use [`dropna`](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?pyspark.sql.DataFrame.dropna) and take the advantage of the `subset` argument. Changing it to

```
def get_if_odd(x):
	if x % 2 == 1:
		return x
        
print data.withColumn(
	'extracted_key',
	udf(get_if_odd, IntegerType())(col('key'))
).dropna(subset=['extracted_key']).collect()```

gives the output:

> [Row(key=1, val=2, extracted_key=1), Row(key=3, val=2, extracted_key=3)]
