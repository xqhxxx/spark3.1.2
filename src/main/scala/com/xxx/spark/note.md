
    RDD （核心  --->          Spark SQL,DS,DF（使用关系查询处理结构化数据，比 RDD 新的 API）
入口：SparkContext            SparkSession


    Spark Streaming ---->   Structured Streaming(（使用数据集和数据帧，比 DStreams 新的 API）
使用DStreams 处理数据流        基于 Spark SQL 引擎构建的可扩展且容错的流处理引擎
入口:StreamingContext         SparkSession



