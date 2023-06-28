data1 = LOAD 'input1.txt' USING PigStorage(',') AS (id:int, value:chararray);
data2 = LOAD 'input2.txt' USING PigStorage(',') AS (id:int, category:chararray);

cogenerated_data = COGENERATE data1 BY id, data2 BY id;

STORE cogenerated_data INTO 'output.txt' USING PigStorage(',');
