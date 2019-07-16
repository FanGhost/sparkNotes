
# fillna() 和na.fill()中的坑
在Join表之前，fillna()和na.fill()是常用的函数，用于防止出错，但是需要注意的是 这两个函数处理列的时候必须要用该列相同的数据类型，否则不会替换null（None）且不会报错，很难发现 例子如下



```python
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession

```


```python
spark = SparkSession.builder.getOrCreate()

dfA = spark.createDataFrame(
    [
        ('a', None),
        ('b', '0'),
        ('c', '0')
    ],
    ('col1', 'col2')
)

dfB = spark.createDataFrame(
    [
        ('a', None, 'x'),
        ('b', 0, 'x')
    ],
    ('col1', 'col2', 'col3')
)
```


```python

print(dfA)

dfA.show()
dfA.fillna("0",subset=['col2']).show()
dfA.na.fill("0").show()
```

    DataFrame[col1: string, col2: string]
    +----+----+
    |col1|col2|
    +----+----+
    |   a|null|
    |   b|   0|
    |   c|   0|
    +----+----+
    
    +----+----+
    |col1|col2|
    +----+----+
    |   a|   0|
    |   b|   0|
    |   c|   0|
    +----+----+
    
    +----+----+
    |col1|col2|
    +----+----+
    |   a|   0|
    |   b|   0|
    |   c|   0|
    +----+----+
    



```python
print(dfB)
dfB.show()
dfB.fillna("0",subset=['col2']).show()
dfB.na.fill("0").show()
```

    DataFrame[col1: string, col2: bigint, col3: string]
    +----+----+----+
    |col1|col2|col3|
    +----+----+----+
    |   a|null|   x|
    |   b|   0|   x|
    +----+----+----+
    
    +----+----+----+
    |col1|col2|col3|
    +----+----+----+
    |   a|null|   x|
    |   b|   0|   x|
    +----+----+----+
    
    +----+----+----+
    |col1|col2|col3|
    +----+----+----+
    |   a|null|   x|
    |   b|   0|   x|
    +----+----+----+
    



```python
dfB.show()
dfB.fillna(0,subset=['col2']).show()
dfB.na.fill(0).show()
```

    +----+----+----+
    |col1|col2|col3|
    +----+----+----+
    |   a|null|   x|
    |   b|   0|   x|
    +----+----+----+
    
    +----+----+----+
    |col1|col2|col3|
    +----+----+----+
    |   a|   0|   x|
    |   b|   0|   x|
    +----+----+----+
    
    +----+----+----+
    |col1|col2|col3|
    +----+----+----+
    |   a|   0|   x|
    |   b|   0|   x|
    +----+----+----+
    


可以看到，

dfA的col2 类型为string，使用字符串'0'可以替换成功。

dfB的col2 类型为bigint，使用字符串'0'不能替换成功，使用int类型的0 能替换成功。


```python

```
