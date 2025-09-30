from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.sql import SparkSession


def get_product_category_pairs(
        products: DataFrame, categories: DataFrame, product_categories: DataFrame
) -> DataFrame:
    p = products.alias("p")
    pc = product_categories.alias("pc")
    c = categories.alias("c")

    return (
        p.join(pc, F.col("p.id") == F.col("pc.product_id"), "left")
        .join(c, F.col("c.id") == F.col("pc.category_id"), "left")
        .select(
            F.col("p.name").alias("product_name"),
            F.col("c.name").alias("category_name"),
        )
        .orderBy(F.col("product_name"))
    )


def main():
    products_data = [
        (1, "Яблоко"),
        (2, "Молоко"),
        (3, "Хлеб"),
    ]

    categories_data = [
        (1, "Фрукты"),
        (2, "Молочные продукты"),
    ]

    product_categories_data = [
        (1, 1),
        (2, 2),
    ]

    spark = SparkSession.builder.appName("ProductsCategories").master("local[*]").getOrCreate()

    products = spark.createDataFrame(products_data, ["id", "name"])
    categories = spark.createDataFrame(categories_data, ["id", "name"])
    product_categories = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])

    # products.show()
    # categories.show()
    # product_categories.show()

    res = get_product_category_pairs(products, categories, product_categories)
    res.show()


if __name__ == "__main__":
    main()
