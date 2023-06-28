data1 = LOAD 'input1.txt' USING PigStorage(',') AS (id:int, value:chararray);
data2 = LOAD 'input2.txt' USING PigStorage(',') AS (id:int, category:chararray);

cogrouped_data = COGROUP data1 BY id, data2 BY id;

STORE cogrouped_data INTO 'output.txt' USING PigStorage(',');
