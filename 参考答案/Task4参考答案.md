# 【Task4】MapReduce+MapReduce执行过程
1. MR原理

hadoop streaming 原理
![图片](https://uploader.shimo.im/f/C3e59L3B4aMHQ5Gf.png!thumbnail)
2. 使用Hadoop Streaming -python写出WordCount

Python版本
1. 准备数据 
vim wc_input.txt
```
hello
word
hello
world
hello
python
java
python
```
```bash
hadoop fs -mkdir -p /user/dw/wc/input/
hadoop fs -mkdir -p /user/dw/wc/output/
hadoop fs -put wc_input.txt /user/dw/wc/input/
``` 
2. 编写map/reduce/run.sh(注意修改/path/to)
mapper
```python
import sys
import re

p = re.compile(r'\w+')
for line in sys.stdin:
	ss = line.strip().split(' ')
	for s in ss:
		if len(p.findall(s))<1:
			continue
		s_low = p.findall(s)[0].lower()
		print(s_low+'\t'+'1')

```

reducer.py

```python
import sys

cur_word = None
sum = 0
for line in sys.stdin:
	word,val = line.strip().split('\t')
	
	if cur_word==None:
		cur_word = word
	if cur_word!=word:
		print('%s\t%s'%(cur_word,sum)) 
		cur_word = word
		sum = 0
	sum+=int(val)
print('%s\t%s'%(cur_word,sum))	
```

run.sh
```bash
HADOOP_CMD="/path/to/hadoop"
STREAM_JAR_PATH="/path/to/hadoop-streaming-2.6.1.jar"

INPUT_FILE_PATH="/user/dw/wc/input/wc_input.txt"
OUTPUT_PATH="/user/dw/wc/output"

$HADOOP_CMD jar $STREAM_JAR_PATH \
    -input $INPUT_FILE_PATH \
    -output $OUTPUT_PATH \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -file ./mapper.py \
    -file ./reducer.py
```
3. 设置输入输出路径
4. 执行，sh run.sh
5. check答案
```bash
hadoop fs -cat /user/dw/wc/output/part-00000
```

1 使用mr计算movielen中每个用户的平均评分。
1.1 主备数据
```bash
wget  wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip
cd ml-100k
mv u.data ml_input.txt
hadoop fs -mkdir -p /user/dw/avgscore/input
hadoop fs -mkdir -p /user/dw/avgscore/output
hadoop fs -put ml_input.txt /user/dw/avgscore/input/
```
1.2 编写map/reduce/run.sh
mapper.py
```python
import sys

for line in sys.stdin:
    item = line.strip().split('\t')
    print(item[1]+'\t'+item[2])

```
reduce.py
```python
import sys

item_score = {}

#Partitoner
for line in sys.stdin:
    line = line.strip()
    item, score = line.split('\t')

    if item in item_score:
        item_score[item].append(int(score))
    else:
        item_score[item] = []
        item_score[item].append(int(score))

#Reducer
for item in item_score.keys():
    ave_score = sum(item_score[item])*1.0 / len(item_score[item])
    print '%s\t%s'% (item, ave_score)

```
run.sh 略，参考以上run.sh
1.3 check数据的正确性，自行编写python代码验证

2. 实现merge功能
2.1 准备数据ml-100k
u.data
u.item
自行使用map先将二者分隔符统一，例如全部使用＃作为分隔符

2.2 编写map/reduce/run
mapper.py
```python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    line = line.split(" ")

    user = "-1"
    item = "-1"
    score = "-1"
    item_name = "-1"
    item_time = "-1"



    if len(line) ==4:
        user = line[0]
        item = line[1]
        score = line[2]
    else:
        item = line[0]
        item_name = line[1]
        item_score = line[2]


    print '%s\t%s\t%s\t%s\t%s' % (user, item, score, item_name, item_time)

```
reducer.py
```python
import sys

item_dict ={}
ui_dict={}
for line in sys.stdin:
    line = line.strip()
    user,item,score,item_name,item_time = line.split('\t')

    if user == "-1":
        item_dict[item] = [item_name,item_time]
    else:
        ui_dict[user] = [item,score]

for user in ui_dict.keys():
    item_name = item_dict[ui_dict[user][0]]
    item_time = item_dict[ui_dict[user][1]]
    item = customer_dict[id][1]
    score = customer_dict[id][2]

    print '%s\t%s\t%s\t%s'% (user, item, score, item_name, item_time)

```

3. 使用mr实现去重任务。
4. 使用mr实现排序。
5. 使用mapreduce实现倒排索引。。
6. 使用mapreduce计算Jaccard相似度。
7. 使用mapreduce实现PageRank。

[Python3调用Hadoop的API](https://www.cnblogs.com/sss4/p/10443497.html)