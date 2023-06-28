--  Lấy dữ liệu gốc từ file csv:
raw_data = LOAD  'hdfs://Master:9000/pig/beer/Beer.csv' using PigStorage(',');

--  Tạo bảng:
structured_data = FOREACH raw_data GENERATE 
            (float) $0 as beer_ABV, 
            (int) $1 as beer_beerID, 
            $3 as beer_name, 
            $4 as beer_style, 
            (float) $7 as review_overall; 

--  Lọc dữ liệu null:
filtered_data = FILTER structured_data BY beer_ABV is not null;
filtered_data = FILTER structured_data BY review_overall is not null;
filtered_data = FILTER structured_data BY beer_beerID is not null;
filtered_data = FILTER structured_data BY beer_style is not null;

--  Chọn 100000 dòng đầu tiên trong bảng:
filtered_data_limit = LIMIT filtered_data 100000;

STORE filtered_data_limit into 'hdfs://Master:9000/pig/output/' using PigStorage(',');