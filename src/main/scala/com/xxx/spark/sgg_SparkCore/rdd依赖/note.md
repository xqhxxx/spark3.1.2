

依赖  血缘关系

阶段划分：



任务划分：
application job stage  task

application 初始化sc
job 一个action算子一个job
stage stage=宽依赖个数+1
task 一个stage中，最后一个rdd分分区个数=task个数

注意：application-->job -->stage --> task 每一层都是1对n的关系

![img.png](img.png)

