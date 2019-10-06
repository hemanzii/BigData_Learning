# 【Task4】MapReduce+MapReduce执行过程

## 原理部分
1. MR的执行过程。
2. 文件读取过程。
3. Map接口的输入及作用。
4. Shuffle过程。
5. Reduce接口的输入及作用。

## 实践部分
以下mapreduce，建议使用Hadoop Streaming + Python实现。
movidelen数据集：wget  wget http://files.grouplens.org/datasets/movielens/ml-100k.zip。

1. 使用Hadoop Streaming 实现 word count。
2. 使用mr计算movielen中每个电影的平均评分（数据：movielen数据集）。
3. 使用mr实现merge功能。根据item，merge movielen中的 u.data u.item（数据：movielen数据集）  

u.data 
|user id | item id | rating | timestamp  |
|  ----  | ----  |  ----  | ----  | 
|196     |242     |3      | 881250949 |  
|186     |302     |3      | 891717742  |  
|22      |377     |1       |878887116 |   
|244     |51      |2       |880606923  |  
|166     |346     |1       |886397596  |  

u.item    
|-|  ----  | ----  |  ----  | ----  | 
|1|Toy Story (1995)|01-Jan-1995|[链接](http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0)|
|2|GoldenEye (1995)|01-Jan-1995|[链接](http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0)|
|3|Four Rooms (1995)|01-Jan-1995|[链接](http://us.imdb.com/M/title-exact?Four%20Rooms%20(1995)|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0)|
|4|Get Shorty (1995)|01-Jan-1995|[链接](http://us.imdb.com/M/title-exact?Get%20Shorty%20(1995)|0|1|0|0|0|1|0|0|1|0|0|0|0|0|0|0|0|0|0)|
|5|Copycat (1995)|01-Jan-1995|[链接](http://us.imdb.com/M/title-exact?Copycat%20(1995)|0|0|0|0|0|0|1|0|1|0|0|0|0|0|0|0|1|0|0)|

merge操作说明，把itemid一致的拼接成一行


4. 使用mr实现去重任务（数据：任务1生成的distict.txt）。

5. 使用mr实现排序(数据：data/randtxtaa+randtxtab)。
6. 使用mapreduce计算Jaccard相似度(数据：movielen数据集)。
note：每个用户看过的电影可以看作一个列表，求每个用户间的Jaccard相似度，取相似度最高的用户的TopK，K=10。
7. 使用mapreduce实现PageRank。


参考: [https://segmentfault.com/a/1190000002672666](https://segmentfault.com/a/1190000002672666)

参考资料：[Python3调用Hadoop的API](https://www.cnblogs.com/sss4/p/10443497.html)
