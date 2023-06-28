data = LOAD 'input3.txt' USING PigStorage(',') AS (category:chararray, subcategory:chararray, value:double);

cubed_report = CUBE data BY (category, subcategory);

STORE cubed_report INTO 'cubed_output.txt' USING PigStorage(',');

rolledup_report = ROLLUP data BY (category, subcategory);

STORE rolledup_report INTO 'rolledup_output.txt' USING PigStorage(',');
