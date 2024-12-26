

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, month, to_date

spark = SparkSession.builder.appName('Sales analysis').getOrCreate()
Sales_df = spark.read.option("delimiter", ";").csv("sales_data.csv", header = True, inferSchema = True)


Sales_df = Sales_df.withColumn("Total_Sales", Sales_df["Quantity"]*Sales_df["Price"])
product_sales = Sales_df.groupBy("Product").sum("Total_Sales")
product_sales=product_sales.withColumnRenamed("sum(Total_Sales)","Total_Sales")

top_products = product_sales.orderBy(col("Total_Sales").desc())

top_product = top_products.limit(1)

Sales_df = Sales_df.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))

Sales_df = Sales_df.withColumn("Month",month(col("Date")))


monthly_sales = Sales_df.groupBy("Month").sum("Total_Sales")
monthly_sales = monthly_sales.withColumnRenamed("sum(Total_Sales)", "Monthly_Sales")


Sales_df.createOrReplaceTempView("Sales")

category = spark.sql("""Select Category , sum(Total_Sales) as Total_sales 
                     from Sales  
                     group by Category
                     order by Total_sales DESC
                     """)


high_sales= spark.sql ("""
Select * from Sales 
where Total_sales > 2000
""")

top_products.write.csv("top_product.csv", header=True)
monthly_sales.write.json("monthly_sales.json")



