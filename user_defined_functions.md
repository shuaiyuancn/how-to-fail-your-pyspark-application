# User Defined Functions (UDFs)

[User Defined Functions](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=udf#pyspark.sql.functions.udf) are powerful. They work on DataFrame column(s) to transform them. They are SQL-oriented functions, so they can work in `select` and other SQL expressions. They are also Python functions, so they can be called in `withColumn`.

## Using UDF with the wrong return data type

Spark is strict on the return data type of UDFs. The following code will fail:


	from pyspark.sql.types import DoubleType
	from pyspark.sql.functions import udf


	def get_an_int():
	    return 1


	dataframe.withColumn(
		'ones',
		udf(get_an_int, DoubleType())()
	)

Because under the hood Spark (or more precisely, Java/Scala) doesn't do the type conversion for you.

Note 

 1. The default return type of `udf` is `StringType`

 2. Most of the data types could accept `None`

