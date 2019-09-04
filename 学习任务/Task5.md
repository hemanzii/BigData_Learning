

# 【Task5】Spark常用API
## 原理部分
1.  初步认识Spark （解决什么问题，基本组件及架构）
2. 为什么比Hadoop快
3. 理解spark的RDD，说一说take,collect,first的区别，为什么不建议使用collect？
4. 理解Spark的shuffle过程
5. 学会阅读Spark源码，整理Spark任务submit过程

## 实践部分
 1. spark集群搭建
 2. 使用pyspark操作Spark，熟悉RDD的基本操作
 3. 使用jupyter连接集群的pyspark 
 4. 学会写wordcount，并提交到spark集群运行。
 5. 使用spark计算《The man of property》中共出现过多少不重复的单词，以及出现次数最多的10个单词。 
 6. 计算出movielen数据集中，平均评分最高的五个电影。
 7. 计算出movielen中，每个用户最喜欢的前5部电影

参考资料：

[远程连接jupyter](https://blog.csdn.net/qq_18293213/article/details/72910834)

【没有jblas库解决办法】

下载jblas包 ：[https://pan.baidu.com/s/1o8w6Wem](https://pan.baidu.com/s/1o8w6Wem)

运行spark-shell时添加jar：spark-shell --jars [jblas path] /jblas-1.2.4.jar
