

***rdd不存储数据。
若果一个rdd要重复使用，会从头再次执行获取数据

rdd对象可以重用 数据无法重用

所以rdd 数据持久化

rdd.cache()
