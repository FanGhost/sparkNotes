
# 多个RDD Join 的问题

在工作中，有时需要连续Join多张表，这在PySpark中Dataframe格式下是不会有问题的。但是如果是 在RDD格式下Join 可能会出现多重tuple的问题，例子如下



```python
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(appName="Example").getOrCreate()
#session = SparkSession.builder.appName("gbenchmark").getOrCreate()
#spark = SparkSession.builder.getOrCreate()
```


```python
pp=(('cat', 2), ('cat', 5), ('book', 4), ('cat', 12))
qq=(("cat",2), ("cup", 5), ("mouse", 4),("cat", 12))
pairRDD1 = sc.parallelize(pp)
pairRDD2 = sc.parallelize(qq)
```


```python
pairRDD1.collect()
```




    [('cat', 2), ('cat', 5), ('book', 4), ('cat', 12)]




```python
pairRDD1.join(pairRDD2).collect()
```




    [('cat', (2, 2)),
     ('cat', (2, 12)),
     ('cat', (5, 2)),
     ('cat', (5, 12)),
     ('cat', (12, 2)),
     ('cat', (12, 12))]




```python
pairRDD1.join(pairRDD2).join(pairRDD2).collect()
```




    [('cat', ((2, 2), 2)),
     ('cat', ((2, 2), 12)),
     ('cat', ((2, 12), 2)),
     ('cat', ((2, 12), 12)),
     ('cat', ((5, 2), 2)),
     ('cat', ((5, 2), 12)),
     ('cat', ((5, 12), 2)),
     ('cat', ((5, 12), 12)),
     ('cat', ((12, 2), 2)),
     ('cat', ((12, 2), 12)),
     ('cat', ((12, 12), 2)),
     ('cat', ((12, 12), 12))]



可以看到 当只Join一个RDD的时候，Value不会出现问题，但是连续Join后作为tuple类型的Value会出现多重，在后面数据处理流程中会出现问题。

这里给出一个解决方案。 可以解决。



```python
pairRDD1.join(pairRDD2).join(pairRDD2).map(lambda x: (x[0],x[1][0]+(x[1][1],)))\
.join(pairRDD2).map(lambda x: (x[0],x[1][0]+(x[1][1],))).collect()
```




    [('cat', (2, 2, 2, 2)),
     ('cat', (2, 2, 2, 12)),
     ('cat', (2, 2, 12, 2)),
     ('cat', (2, 2, 12, 12)),
     ('cat', (2, 12, 2, 2)),
     ('cat', (2, 12, 2, 12)),
     ('cat', (2, 12, 12, 2)),
     ('cat', (2, 12, 12, 12)),
     ('cat', (5, 2, 2, 2)),
     ('cat', (5, 2, 2, 12)),
     ('cat', (5, 2, 12, 2)),
     ('cat', (5, 2, 12, 12)),
     ('cat', (5, 12, 2, 2)),
     ('cat', (5, 12, 2, 12)),
     ('cat', (5, 12, 12, 2)),
     ('cat', (5, 12, 12, 12)),
     ('cat', (12, 2, 2, 2)),
     ('cat', (12, 2, 2, 12)),
     ('cat', (12, 2, 12, 2)),
     ('cat', (12, 2, 12, 12)),
     ('cat', (12, 12, 2, 2)),
     ('cat', (12, 12, 2, 12)),
     ('cat', (12, 12, 12, 2)),
     ('cat', (12, 12, 12, 12))]




```python

```
