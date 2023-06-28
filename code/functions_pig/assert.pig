data = LOAD 'input4.txt' USING PigStorage(',') AS (id:int, value:int);

ASSERT data BY (value > 0) 'Invalid value found!';

STORE data INTO 'output.txt' USING PigStorage(',');
