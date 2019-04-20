# BigData_Learning

# 【Task1】创建虚拟机+熟悉linux(2day)

1. [创建三台虚拟机](https://mp.weixin.qq.com/s/WkjX8qz7nYvuX4k9vaCdZQ) 
2. 在本机使用Xshell连接虚拟机 
3. [CentOS7配置阿里云yum源和EPEL源](https://www.cnblogs.com/jimboi/p/8437788.html)
4. 安装jdk 
5. 熟悉linux 常用命令  
6. shell 变量/函数

注意：
* 电脑系统需要64位(4g+)
* 三台虚拟机的运行内存不能超过电脑的运行内存
* 三台虚拟机ip不能一样，否则会有冲突、



参考资料：
 1. [安装ifconfig](https://jingyan.baidu.com/article/363872ec26bd0f6e4aa16f59.html)
 2. [bash: wget: command not found的两种解决方法](https://www.cnblogs.com/areyouready/p/8909665.html)
 3. linux系统下载ssh服务
 4. [关闭windows的防火墙！如果不关闭防火墙的话，可能和虚拟机直接无法ping通！](https://www.linuxidc.com/Linux/2017-11/148427.htm)
 5. 大数据软件 ：链接：[https://pan.baidu.com/s/17fEq3IPVoeE29cWCrSpO8Q](https://pan.baidu.com/s/17fEq3IPVoeE29cWCrSpO8Q) 提取码：finf 


# 【Task 2】搭建Hadoop集群(3day)

1. 搭建HA的Hadoop集群并验证，3节点(1主2从)，理解HA/Federation
2. 阅读Google三大论文
3. Hadoop的作用（解决了什么问题）/运行模式/基础组件及架构
4. 学会阅读HDFS源码，并自己阅读一段HDFS的源码，推荐HDFS上传/下载过程
5. Hadoop中各个组件的通信方式，RPC/Http等
6. 学会写WordCount（Java/Python-Hadoop Streaming），理解分布式/单机运行模式的区别
7. 理解MapReduce的执行过程
8. Yarn在Hadoop中的作用

参考资料：
[Google三大论文](https://blog.csdn.net/w1573007/article/details/52966742)

集群规划1-省机器：

![image](./pic/集群规划1.jpg)
 
集群规划2-清晰：

![image](./pic/集群规划2.jpg)


# 【Task 3】HDFS常用命令/API+上传下载过程
1. 认识HDFS
2. 熟悉hdfs常用命令
3. Python操作HDFS的其他API
4. 观察上传后的文件，上传大于128M的文件与小于128M的文件有何区别？
5. 启动HDFS后，会分别启动NameNode/DataNode/SecondaryNameNode，这些进程的的作用分别是什么？
6. NameNode是如何组织文件中的元信息的，edits log与fsImage的区别？使用hdfs oiv命令观察HDFS上的文件的metadata
7. SecondaryNameNode作用是什么？


参考: [https://segmentfault.com/a/1190000002672666](https://segmentfault.com/a/1190000002672666)

参考资料：[Python3调用Hadoop的API](https://www.cnblogs.com/sss4/p/10443497.html)



# 【Task5】Spark常用API
 1. spark集群搭建
 2. 初步认识Spark （解决什么问题，为什么比Hadoop快，基本组件及架构Driver/）
 3. 理解spark的RDD
 4. 使用shell方式操作Spark，熟悉RDD的基本操作
 5. 学会阅读Spark源码，整理Spark任务submit过程
 6. 理解Spark的shuffle过程
 7. 学会使用SparkStreaming
 8. 说一说take,collect,first的区别，为什么不建议使用collect？(虽然我用了。。。)
 9. 向集群提交Spark程序
 10. 使用spark计算《The man of property》中共出现过多少不重复的单词，以及出现次数最多的10个单词。 
 11. 计算出movielen数据集中，平均评分最高的五个电影。
 12. 计算出movielen中，每个用户最喜欢的前5部电影

参考资料：

[远程连接jupyter](https://blog.csdn.net/qq_18293213/article/details/72910834)

【没有jblas库解决办法】

下载jblas包 ：[https://pan.baidu.com/s/1o8w6Wem](https://pan.baidu.com/s/1o8w6Wem)

运行spark-shell时添加jar：spark-shell --jars [jblas path] /jblas-1.2.4.jar


# 【Task6】Hive原理及其使用
1. 安装MSQL、Hive
2. 采用MySQL作为hive元数据库
3. Hive与传统RDBMS的区别
4. HIve原理及架构图
5. HQL的基本操作（Hive中的SQL）
6. Hive内部表/外部表/分区


参考资料：[https://www.shiyanlou.com/courses/running](https://www.shiyanlou.com/courses/running)  

[MySQL安装](https://blog.csdn.net/z13615480737/article/details/78906598)

[采用MySQL作为hive元数据库](https://blog.csdn.net/u010003835/article/details/80324038)

[HiveonSpark环境部署](https://www.cnblogs.com/xinfang520/p/7684605.html)

[https://blog.csdn.net/qq_26937525/article/details/54136317](https://blog.csdn.net/qq_26937525/article/details/54136317)

[https://docs.azure.cn/zh-cn/hdinsight/hadoop/python-udf-hdinsight](https://docs.azure.cn/zh-cn/hdinsight/hadoop/python-udf-hdinsight)




# 【Task7】实践
1. 计算每个content的CTR。
数据集下载：链接：https://pan.baidu.com/s/1YDvBWp35xKLg5zsysEjDGA 提取码：rpgs 
2. 【选做】 使用Spark实现ALS矩阵分解算法，
movielen 数据集：http://files.grouplens.org/datasets/movielens/ml-100k.zip
 [基于ALS矩阵分解算法的Spark推荐引擎实现](https://www.cnblogs.com/muchen/p/6882465.html)
3. 使用Spark分析Amazon DataSet(实现 Spark LR、Spark TFIDF)
数据集：[http://jmcauley.ucsd.edu/data/amazon/](http://jmcauley.ucsd.edu/data/amazon/)
**preprocess**
	* Spark LR
	* Spark TFIDF