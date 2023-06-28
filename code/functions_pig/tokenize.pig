data = LOAD 'hdfs://trungnghia-master:9000/pig/beer/input5.txt' USING PigStorage(',') AS (A:chararray, B:chararray);

TOKEN = FOREACH data GENERATE A, TOKENIZE(A);

dump TOKEN;