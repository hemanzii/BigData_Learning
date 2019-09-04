# 【Task 3】HDFS常用命令/API+上传下载过程
## 原理部分
1. HDFS是用来解决什么问题的
2. HDFS设计与架构
3. 理解NameNode/DataNode/SecondaryNameNode的作用
4. 理解HA高可用与Federation联邦
5. HDFS文件上传下载过程，源码阅读与步骤整理，并画图。

## 实践部分
1. 熟悉hdfs常用命令，在交互式命令行中操作hdfs。
2. Python操作HDFS的其他API。
3. 验证HDFS的分块存储机制：观察上传后的文件，上传大于128M的文件与小于128M的文件有何区别？
4. 验证NameNode的作用：NameNode是如何组织文件中的元信息的，edits log与fsImage的区别？使用hdfs oiv命令观察HDFS上的文件的metadata



参考: [https://segmentfault.com/a/1190000002672666](https://segmentfault.com/a/1190000002672666)

参考资料：[Python3调用Hadoop的API](https://www.cnblogs.com/sss4/p/10443497.html)
