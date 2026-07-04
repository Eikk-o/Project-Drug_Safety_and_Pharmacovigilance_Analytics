def write_to_mongo(df, database, collection, uri="mongodb://mongo:27017"):
    df.write.format("mongodb") \
        .mode("overwrite") \
        .option("connection.uri", uri) \
        .option("database", database) \
        .option("collection", collection) \
        .save()