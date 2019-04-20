






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
![在这里插入图片描述](https://img-blog.csdnimg.cn/2019042013552856.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)


集群规划2-清晰：
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420135536417.jpg?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)


# 【Task 3】HDFS常用命令/API+上传下载过程
1. 认识HDFS
2. 熟悉hdfs常用命令
3. Python操作HDFS的其他API
4. 观察上传后的文件，上传大于128M的文件与小于128M的文件有何区别？
5. 启动HDFS后，会分别启动NameNode/DataNode/SecondaryNameNode，这些进程的的作用分别是什么？
6. NameNode是如何组织文件中的元信息的，edits log与fsImage的区别？使用hdfs oiv命令观察HDFS上的文件的metadata
7. SecondaryNameNode作用是什么？


```
hadoop fs -mkdir /test
hadoop fs- mkdir  /user/test 递归的创建目录，如果上级目录不存在，自动创建
hadoop fs -put < local file > < hdfs file > 上传文件
hadoop fs -get < hdfs file > < local file or dir> 下载文件
hadoop fs -copyToLocal < local src > ... < hdfs dst >
hadoop fs -mv < hdfs file >  < hdfs file > 重命名或移动
hadoop fs -ls  / 列出文件
hadoop fs -ls -R / 递归的列出文件
hadoop fs -rm -R /user/* 递归删除文件,删除/user/下的所有文件
hadoop fs -count < hdfs path > 统计hdfs对应路径下的目录个数，文件个数，文件总计大小
hadoop fs -du < hdsf path>  显示hdfs对应路径下每个文件夹和文件的大小
hdoop fs -stat [format] < hdfs path >
hadoop archive -archiveName name.har -p < hdfs parent dir > < src >* < hdfs dst > 压缩归档
```

参考: [https://segmentfault.com/a/1190000002672666](https://segmentfault.com/a/1190000002672666)
6. API操作


```
#coding=utf8
import pyhdfs
fs = pyhdfs.HdfsClient(hosts='localhost,50070',user_name='sheldonwong')
fs.get_home_directory()#返回这个用户的根目录
fs.get_active_namenode()#返回可用的namenode节点

resp = fs.listdir('/')
print(','.join(resp))
```

Task：在Python练习其他HDFS的其他API
4. 观察上传后的文件，上传大于128M的文件与小于128M的文件有何区别？

在Hadoop中，大文件的存储需要分块，分块的大小可以配置，配置项为，默认的是配置是128M。
5. 启动HDFS后，会分别启动NameNode/DataNode/SecondaryNameNode，这些进程的的作用分别是什么？
6. NameNode是如何组织文件中的元信息的，edits log与fsImage的区别？使用hdfs oiv命令观察HDFS上的文件的metadata
7. SecondaryNameNode作用是什么？
# 【Task4】MapReduce+MapReduce执行过程
1. 理解MapReduce的执行过程
2. 使用Hadoop Streaming -python写出WordCount
3. 使用mr计算movielen中每个用户的平均评分。
4. 使用mr实现去重任务。
5. 使用mr实现排序。
6. 使用mapreduce实现倒排索引。
7. 使用mr实现join功能。
8. 使用mapreduce计算Jaccard相似度。
9. 使用mapreduce实现PageRank。


参考资料：[Python3调用Hadoop的API](https://www.cnblogs.com/sss4/p/10443497.html)

hadoop streaming 原理
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420093627869.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)
10. 使用Hadoop Streaming -python写出WordCount

**Python版本**
mapper
```
import sys
import re

p = re.compile(r'\w+')
for line in sys.stdin:
	ss = line.strip().split(' ')
	for s in ss:
		if len(p.findall(s))<1:
			continue
		s_low = p.findall(s)[0].lower()
		print s_low+'\t'+'1'

```

reducer.py
```
import sys
cur_word = None
sum = 0
for line in sys.stdin:
	word,val = line.strip().split('\t')
	if cur_word==None:
		cur_word = word
	if cur_word!=word:
		print ('%s\t%s'%(cur_word,sum))
		cur_word = word
		sum = 0
	sum+=int(val)
print( '%s\t%s'%(cur_word,sum)	)
```

run.sh
```
HADOOP_CMD="/path/to/hadoop"
STREAM_JAR_PATH="/path/to/hadoop-streaming-2.6.1.jar"

INPUT_FILE_PATH_1="/inputt"
OUTPUT_PATH="/output/wc"

$HADOOP_CMD jar $STREAM_JAR_PATH \
    -input $INPUT_FILE_PATH_1 \
    -output $OUTPUT_PATH \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -file ./mapper.py \
    -file ./reducer.py
```

1. 使用mr计算movielen中每个用户的平均评分。
2. 使用mr实现去重任务。
3. 使用mr实现排序。
4. 使用mapreduce实现倒排索引。
5. 使用mr实现join功能。
6. 使用mapreduce计算Jaccard相似度。
7. 使用mapreduce实现PageRank。
8. HiveUDF

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




 13. 认识Spark
	* Spark的组件
	Driver
	Cluster Manager
	Worker
	* Spark的运行模式
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420084515694.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)
3. Spark的RDD
RDD是弹性分布式数据集。
弹性的意思是当保存RDD的一台机器遇到错误时，Spark可以根据lineage谱系图重新计算出这些RDD。
分布式的意思是，这些对象集合（分区）被分布式的存储在集群中的不同节点上。
Spark 中的 RDD 就是一个不可变的分布式对象集合。每个 RDD 都被分为多个分区，这些分区在集群中的不同节点上。
初次之外，RDD还包含了一些操作API，例如常见的map，reduce，filter等。


4.  使用shell方式操作Spark

Tips：建议先准备在home目录下新建workspace/learnspark/目录

```
cd ~
mkdir -r workspace/learnspark/
cd workspace/learnspark/
vim test.txt # 输入i进入编辑模式
```
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420084627732.png)
保存退出(esc+:wq)

* 进入交互式编程环境，打开终端，在终端输入pyspark，便可进入pyspark环境。进入pyspark环境后，会初始化好SparkContext。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420084831814.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)

* 创建rdd
通常有两种方式创建RDD，一种是读取外部数据集，另外一种是使用已有的对象集合。
 	* 使用已有的对象集合
 	![在这里插入图片描述](https://img-blog.csdnimg.cn/2019042008483889.png)
 	* 读取外部数据集(注意更换目录)
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420084909406.png)
5. 基础的API操作
为了简便，下面如果未声明，均使用第一种方式创建rdd，这也是在工作中快速验证rdd API功能的最简便快捷的方式。
* map：数据集中的每个元素经过用户自定义的函数转换形成一个新的RDD
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420084935822.png)
我们自定义了一个lambda函数，使得每个元素+1.
那么，这和普通的python list操作有什么区别的？试想，当list的元素在百万，千万级别的时候，python还尚能处理这种逻辑，假如集合中的元素是百亿，千亿级别呢，这个时候就需要依靠Spark处理了。这也正是RDD要解决的问题，海量大数据处理。
map示意图，其中红色方框代表分区
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420084953418.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)
* union 两个rdd的并集
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085007665.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)
* intersection 两个rdd的交集，注意这里声明了一个新的rdd7= 5，2，1和rdd5=5，2，0的交集是5，2
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085104453.png)
* filter: 过滤，筛选出符合条件的元素
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085110826.png)
* distinct：去重
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085121126.png)
* cartesian：笛卡儿积
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085142907.png)
* reduce： 将rdd中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085152787.png)
* groupByKey：对于kv类型的数据，按key分组
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420085202985.png)

需要说明的是，spark RDD的算子分为两种，一种是transformation，例如map，filter；另外一种是action，例如collect(),count(),groupByKey()等。**只有action算子才会真正的触发计算**。

**RDD API问题与作业**
1. 尽可能多的练习RDD的其他API，包括sortBy,join,reduceByKey,take,first,count,countByKey等。
2. 整理自己学习过的RDD算子，并给他们按照transformation和action进行归类，画出思维导图。
3. 说一说take,collect,first的区别，为什么不建议使用collect？(虽然我用了。。。)
1. 向集群提交Spark程序

可以在jupyter book中使用spark，也可以在pySpark中直接使用（会自动创建spark context）
不过更常用的是，将job提交到集群中运行。
6. 先来体验一下Spark版本的wordcount吧
* wc.py
```
import os
from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName('word count')
sc = SparkContext(conf = conf)

#注意将这里改成自己的路径 
text_file = sc.textFile('hdfs:///test/The_Man_of_Property.txt')
 
count = text_file.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b).sortBy(ascending=False, numPartitions=None, keyfunc = lambda x: x[1]) 
#拿出一些结果先看看
print(count.take(10))
#保存结果
count_rdd.saveAsTextFile('hdfs:///datawhale/out/')
```

**提交：**

```
bin/spark-submit --master spark://host:port --executor-memory 2g wc.py
```

* wordcount 的另外一种写法
```
def splitx():
    return line.split(" ")
count = textfile.flatMap(splitx) \
                    .map(lambda word:(word,1)) \
                    .reduceByKey(lambda a,b:a+b)
```
这里想说明的是，map/reduce等算子，不仅可以接受lamda函数作为参数，也可以接受自定义的函数，直接把函数名传进去即可。


7. 任务：
* 使用上述API计算《The man of property》中共出现过多少不重复的单词，以及出现次数最多的10个单词。
movielen 数据集：http://files.grouplens.org/datasets/movielens/ml-100k.zip
* 计算出movielen中，每个用户最喜欢的前5部电影。
运行pyspark
```
import pandas as pd
# 读取文件
user_data = sc.textFile("/test/users.dat")
movie_data = sc.textFile("/test/movies.dat")
ratings_data = sc.textFile("/test/ratings.dat")
# 切分数据
user_rdd = user_data.map(lambda line: line.split("::"))
movie_rdd = movie_data.map(lambda line: line.split("::"))
ratings_rdd = ratings_data.map(lambda line: line.split("::"))
# 将RDD转化为DF
user_df = sqlContext.createDataFrame(user_rdd).toPandas()
movie_df = sqlContext.createDataFrame(movie_rdd).toPandas()
ratings_df = sqlContext.createDataFrame(ratings_rdd).toPandas()
# Rename列名
user_df.columns = ['UserID','Gender',"Age","Occupation","Zip-code"]
ratings_df.columns = ['UserID','MovieID',"Rating","Timestamp"]
movie_df.columns = ['MovieID','Title',"Genres"]
#将三张表合并成一张表
total_df = pd.merge(ratings_df,user_df,on = ["UserID"],how = "right")
total_df = pd.merge(total_df,movie_df,on = ["MovieID"],how = "left")
# 聚合操作
c = total_df["Title"].groupby(total_df["UserID"])
# 取出前5ge
second = c.agg(lambda x: x.value_counts().index[1]).reset_index()
first = c.agg(lambda x: x.value_counts().index[0]).reset_index()
third = c.agg(lambda x: x.value_counts().index[2]).reset_index()
fourth = c.agg(lambda x: x.value_counts().index[3]).reset_index()
fifth = c.agg(lambda x: x.value_counts().index[4]).reset_index()
# 创建新的的DF
like = pd.DataFrame()
like["UserID"] = first.UserID
like["first"] = first.Title
like["second"] = second.Title
like["third"] = third.Title
like["fourth"] = fourth.Title
like["fifth"] = fifth.Title
print(like[:5])
```
* 计算出movielen数据集中，平均评分最高的五个电影。
```
total_df.groupby('Title')['Rating'].mean().reset_index().sort_values("Rating",ascending = False)[:5]
```
* 【选做】 计算出movielen用户的行为相似度（相似度采用Jaccard相似度）。

8. 参考资料：
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

 1. [MySQL安装](https://blog.csdn.net/z13615480737/article/details/78906598)
    	[采用MySQL作为hive元数据库](https://blog.csdn.net/u010003835/article/details/80324038)
    	[HiveonSpark环境部署](https://www.cnblogs.com/xinfang520/p/7684605.html)
 2. 认识Hive

Hive通常用于数据仓库，可存储/查询结构化数据等，其优势在于存储量极大，可以存储数百亿/千亿（TB/PB）级别的数据。相较于传统的RDBMS，只能存储千万/亿级数据，有很大优势。
其底层存储依赖于HDFS，因此可以水平扩展，
元信息存储在derby或mysql中，
查询使用HQL，底层使用MapReduce实现。
2. Hive与传统RDBMS的区别
![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420083012676.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)

3. HIve原理及架构图

![在这里插入图片描述](https://img-blog.csdnimg.cn/20190420083027681.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L0hlaXRhbzUyMDA=,size_16,color_FFFFFF,t_70)

 - 用户接口主要有三个： CLI， Client 和 WUI。其中最常用的是 CLI， Cli 启动的时候， 会同时启动一个 Hive副本。Client 是 Hive 的客户端，用户连接至 Hive Server。 在启动 Client 模式的时候，需要指出 Hive    Server 所在节点，并且在该节点启动 Hive Server。 WUI 是通过浏览器访问 Hive。
 * 元数据：Hive将元数据存储在数据库中，如 mysql、 derby。
   Hive中的元数据包括表的名字，表的列和分区及其属性，表的属性（是否为外部表等），表的数据所在目录等。

  * HQL：解释器、编译器、优化器完成HQL 查询语句从词法分析、语法分析、编译、优化以及查询计划的生成。生成的查询计划存储在 HDFS 中，并在随后有 MapReduce调用执行。
   * Hadoop：Hive 的数据存储在 HDFS 中，大部分的查询由 MapReduce 完成（包含 * 的查询，比如 select * from tbl 不会生成 MapRedcue 任务）。

4. HQL（Hive中的SQL）

**准备数据**
**建表**

```
CREATE EXTERNAL TABLE datawhale.user_action
(id INT,
uid STRING,
item_id STRING,
behavior_type INT,
item_category STRING,
visit_date DATE,
province STRING) COMMENT 'Welcome to datawhale!' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/datawhale/user_action';
```

也可以先建表，再载入数据。

* 查看存在的表

```
show tables;
```

* 查看表结构

```
desc datawhale.user_action;
```

* 筛选10条看看

```
select * from datawhale.user_action limit 10;
```

* 查询表内共多少行数据

```
select count(*) from datawhale.user_action;
```
* 查询不重复的uid共多少行

```
select count(distinct uid) from datawhale.user_action;
```

* 查询2014年12月10日到2014年12月14日有多少人浏览了商品

```
select count(*) from datawhale.user_action where behavior_type='1' and visit_date<='2014-12-14' and visit_date>='2014-12-10';
```

* 查询一件商品的购买率
* 查询某一天在该网站购买商品超过5次的用户id

```
select uid from datawhale.user_action where behavior_type='4' and visit_date='2014-12-12' group by uid having count(behavior_type='4')>5;
```

* 创建中间表，存储每个地区有多少用户浏览了网站

```
create table datawhale.province_user_dist(
province STRING,
view INT) 
COMMENT 'This is the count of user of each city' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY '\t' STORED AS TEXTFILE;
```

* 覆盖式的导入数据

```
insert overwrite table datawhale.provice_user_count
select province,count(behavior_type)
from datawhale.user_action 
where behavior_type='1' group by province;
```
Tips：试想，如果你的表特别的大，有没有什么可以优化的地方？
当然有，比如分区分桶，分区分桶可以从一定程度上提高查询效率。


5. Hive内部表/外部表/分区

**内部表（managed table）**
* 默认创建的是内部表（managed table），存储位置在hive.metastore.warehouse.dir设置，默认位置是/user/hive/warehouse。
* 导入数据的时候是将文件剪切（移动）到指定位置，即原有路径下文件不再存在
* 删除表的时候，数据和元数据都将被删除
* 默认创建的就是内部表create table xxx (xx xxx)

**外部表（external table）**
* 外部表文件可以在外部系统上，只要有访问权限就可以
* 外部表导入文件时不移动文件，仅仅是添加一个metadata
* 删除外部表时原数据不会被删除
* 分辨外部表内部表可以使用DESCRIBE FORMATTED table_name命令查看
* 创建外部表命令添加一个external即可，即create external table xxx (xxx)
* 外部表指向的数据发生变化的时候会自动更新，不用特殊处理

**表分区（Partitioned table）**
* 有些时候数据是有组织的，比方按日期/类型等分类，而查询数据的时候也经常只关心部分数据，比方说我只想查2017年8月8号，此时可以创建分区

**数据准备：**
movielen
user item rate thedate
**分区表的创建：**

```
create table datawhale.user_action_partition(
user      int comment "user id",
item      int comment "teim_id",
rate      int comment "rate" )
partitioned by (thedate string);
```

* 查询分区；

```
show partitions datawhale.user_action_partition;
```

* 插入分区

* 删除分区。

```
ALTER TABLE  table_name DROP PARTITION (thedate='20000101')
```

* 分区表的查询。

思考一下，为什么要使用分区表？

参考:https://www.cnblogs.com/wswang/p/7718103.html

6. HiveUDF

**demo1：**
这里用python自定义函数，去实现一个方法，利用身份证号去判断性别(18位身份证的倒数第二位偶数为女，奇数为男.15位身份证的倒数第一位偶数为女,奇数为男.).其实这个需求可以使用hive自带的function去进行解决.我们接下来使用2种方式去实现这个需求.
* 数据

```
neil    411326199402110030
pony    41132519950911004x
jcak    12312423454556561
tony    412345671234908
```

* HQL：

```
select idcard,
case when length(idcard) = 18 then
             case when substring(idcard,-2,1) % 2 = 1 then '男' 
             when substring(idcard,-2,1) % 2 = 0 then '女' 
             else 'unknown' end 
     when length(idcard) = 15 then 
            case when substring(idcard,-1,1) % 2 = 1 then '男'
            when substring(idcard,-1,1) % 2 = 0 then '女'
            else 'unknown' end
     else '不合法' end 
from person;
```

* hive udf：

```
# -*- coding: utf-8 -*-
import sys

for line in sys.stdin:
    detail = line.strip().split("\t")
    if len(detail) != 2:
        continue
    else:
        name = detail[0]
        idcard = detail[1]
        if len(idcard) == 15:
            if int(idcard[-1]) % 2 == 0:
                print("\t".join([name,idcard,"女"]))
            else:
                print("\t".join([name,idcard,"男"]))
        elif len(idcard) == 18:
            if int(idcard[-2]) % 2 == 0:
                print("\t".join([name,idcard,"女"]))
            else:
                print("\t".join([name,idcard,"男"]))
        else:
            print("\t".join([name,idcard,"身份信息不合法!"]))
```

===测试

```
cat person.txt|python person.py
```

===udf

```
add file hiveudf.py
select transform(name,idcard) USING 'python person.py'  AS (name,idcard,gender) from person;
```

1. 文件开头的 add file 语句将 hiveudf.py 文件添加到分布式缓存，使群集中的所有节点都可访问该文件。
2. SELECT TRANSFORM ... USING 语句从 hivesampletable 中选择数据。 它还将 name、idcard值传递到 hiveudf.py 脚本。
3. AS 子句描述从 hiveudf.py 返回的字段。

脚本文件：
1. 从 STDIN 读取一行数据。
2. 尾随的换行符使用 string.strip()删除。
3. 执行流式处理时，一个行就包含了所有值，每两个值之间有一个制表符。 因此， string.split("\t") 可用于在每个制表符处拆分输入，并只返回字段。
4. 在处理完成后，必须将输出以单行形式写入到 STDOUT，并在每两个字段之间提供一个制表符。



参考：
1. [https://blog.csdn.net/qq_26937525/article/details/54136317](https://blog.csdn.net/qq_26937525/article/details/54136317)
2. [https://docs.azure.cn/zh-cn/hdinsight/hadoop/python-udf-hdinsight](https://docs.azure.cn/zh-cn/hdinsight/hadoop/python-udf-hdinsight)




# 【Task7】实践
1. 计算每个content的CTR。
2. 【选做】 使用Spark实现ALS矩阵分解算法，
3. 使用Spark分析Amazon DataSet(实现 Spark LR、Spark TFIDF)


4. 计算每个content的CTR。
数据集下载：链接：https://pan.baidu.com/s/1YDvBWp35xKLg5zsysEjDGA 提取码：rpgs 

|uid  |  用户ID|
|--|--|
|  content_list|  内容id|
|  content_id| 被点击的内容id |

2. 【选做】 使用Spark实现ALS矩阵分解算法，
movielen 数据集：http://files.grouplens.org/datasets/movielens/ml-100k.zip
 [基于ALS矩阵分解算法的Spark推荐引擎实现](https://www.cnblogs.com/muchen/p/6882465.html)

4. 使用Spark分析Amazon DataSet
数据集：[http://jmcauley.ucsd.edu/data/amazon/](http://jmcauley.ucsd.edu/data/amazon/)
**preprocess**
	* Spark LR
	* Spark TFIDF