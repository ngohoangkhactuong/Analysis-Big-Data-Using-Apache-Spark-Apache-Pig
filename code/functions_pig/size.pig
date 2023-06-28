data = LOAD 'hdfs://trungnghia-master:9000/pig/beer/input5.txt' USING PigStorage(',') AS (A:chararray, B:chararray);

CONCATAandB = FOREACH data GENERATE A, B, CONCAT(A,B) AS C;

SIZE = FOREACH CONCATAandB GENERATE C, SIZE(C) AS CSIZE;

dump SIZE;