# 【Task4】MapReduce+MapReduce执行过程
1. MR原理，包含文件读取过程，Map接口的输入及作用，Shuffle过程，Reduce接口的输入及作用。
以下mapreduce，建议使用Hadoop Streaming + Python实现。
movidelen数据集：wget  wget http://files.grouplens.org/datasets/movielens/ml-100k.zip。
2. 使用Hadoop Streaming 实现 word count。
3. 使用mr计算movielen中每个电影的平均评分（数据：movielen数据集）。
4. 使用mr实现merge功能。根据item，merge movielen中的 u.data u.item（数据：movielen数据集）
5. 使用mr实现去重任务（数据：distict.txt）。
6. 使用mr实现排序(数据：sortdata.txt)。
7. 使用mapreduce实现倒排索引。
8. 使用mapreduce计算Jaccard相似度(数据：movielen数据集)。
note：每个用户看过的电影可以看作一个列表，求每个用户间的Jaccard相似度，取相似度最高的用户的TopK，K=10。
9. 使用mapreduce实现PageRank。


参考: [https://segmentfault.com/a/1190000002672666](https://segmentfault.com/a/1190000002672666)

参考资料：[Python3调用Hadoop的API](https://www.cnblogs.com/sss4/p/10443497.html)
