# Schibsted Data Engineer Data Processing Assignment

On running the script a new file will be generated in data directory with name [urls-output.json](data/urls-output.json)

Databrick Notebook: [Click Here](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4464596420022593/1911053387610219/103040325764354/latest.html)

## Introduction
As a data engineer at Schibsted you will be tasked with writing data processing pipelines transforming all sorts of data. One of the common data points are URLs. In this assignment you will be wrting the logic to parse the raw URLs and extract the key data points from them. This assignment is based on an actual job implemented in the Common Data Warehouse.

## Data
You will find the data in [data/urls.json](data/urls.json). It's stored in the JSON lines format and has 5000 records. The first 100 records are a sample to show how correctly parsed data should look like.

If you are not familiar with the structure of the URL, you can check out the [URI specification (RFC 3986)](https://datatracker.ietf.org/doc/html/rfc3986). There's also a nice overview on Wikipedia ([Uniform Resource Identifier](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier)).

### Schema
* `row_id: int`
* `is_sample: boolean` - is the record part of the example with all parsed fields?
* `raw_url: string` - raw URL that you need to process
* `scheme: string` - URI access scheme (provided for sample recrods only)
* `domain: string`- domain part of the URL (provided for sample recrods only)
* `path: string` - url-decoded path part of the URL (provided for sample recrods only)
* `fragment: string` - url-decoded fragment part of the URL (provided for sample recrods only)
* `query_params_array: array` - url-decoded query parameters parsed from the URL (provided for sample recrods only)
    * `element: struct`
        * `name: string` - name of the query parameter
        * `value: string` - value of the query parameter

## Tools
For this assignment you will need to have access to running PySpark. You don't need to run it on a cluster, a locally running Spark is sufficient because the provided dataset is tiny. Spark 3.0+ is recommended.

You could also use [Databricks Community Edition](https://docs.databricks.com/getting-started/try-databricks.html#sign-up-for-community-edition) to work on this assignment, if you're familiar with Databricks or want to give it a go. Our team is using Databricks to run our Spark pipelines.

## Task
1. Open the starter code in [src/url_parsing/url_parser.py](src/url_parsing/url_parser.py). It's a PySpark application that is supposed to read the provided data, process the raw urls, and write the trasformed dataset.
2. Modify the `get_spark()` and `main()` functions to make sure you run the provided code in the Spark environment available to you.
3. Write the implementation of the `parse_urls()` function:
    * take the `raw_url` field and extract `scheme`, `domain`, `path`, `fragment`, and the query string;
    * url-decode the values of `path` and `fragment`;
    * split the query string into param=value pairs and url decode the value;
    * collect the param name and value pairs into an array of structs `query_params_array`;
    * return the transformed dataframe with the new fields, schema should be the same as in [Schema](#schema)
4. Use the provided sample records to test your processing logic.
5. Write the transformed dataframe into a JSON lines file, name it `urls-output.json`.
6. Package your code and the output data into a zip file and send it back to your contact person at Schibsted.

### Implementation hints
* It's possible to implement the solution using built-in Spark functions, so UDFs are not necessary.
* Some of the functions you should use are not exposed in the python API but are available in the SQL API: [list of SQL functions](https://spark.apache.org/docs/latest/api/sql/index.html).
* [`pyspark.sql.functions.expr()`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.expr.html#pyspark.sql.functions.expr) can be used to call SQL functions.
* You can run Java methods using the [`java_method()`](https://spark.apache.org/docs/latest/api/sql/index.html#java_method) function. It'll come in handy to url-decode the values. Java has a class [`java.net.URLDecoder`](https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/net/URLDecoder.html) with a `decode` method.
* To apply a function on every element of an array you can use the [`transform()`](https://spark.apache.org/docs/latest/api/sql/index.html#transform) sql function.

### Coding guidelines
The code should be straightforward and self-explanatory. Avoid complex one-liners and prefer well structured and easy to read block of code. In addition, it should be easy to write unit tests for the trasformations, so try split your code into functions that do one thing. Finally, add docstrings and type hints so that the code is self-documented. Of course, sometimes a function name is obvious enough to not require a docstring, in which case adding one might add extra noise to the code. Use your own discretion.
