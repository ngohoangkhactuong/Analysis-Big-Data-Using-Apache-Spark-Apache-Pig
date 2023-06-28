--Lấy dữ liệu gốc từ file csv và tạo bảng:

raw_data = LOAD  'hdfs://hadoop-master:9820/beer/Beer.csv' using PigStorage(',');
structured_data = FOREACH raw_data GENERATE (float) $0 as beer_ABV, (int) $1 as beer_beerID, (int) $2 as beer_brewerID, $3 as beer_name, $4 as beer_style, (float) $5 as review_appearance, (float) $6 as review_palette, (float) $7 as review_overall, (float) $8 as review_taste, $9 as review_profileName;

--Lọc dữ liệu chọn và chọn 100000 dòng đầu tiên trong bảng

filtered_data = FILTER structured_data BY review_overall is not null;
filtered_data = FILTER structured_data BY beer_beerID is not null;
filtered_data_limit = LIMIT filtered_data 100000;

--Duyệt các thành phần trong filtered_data_limit 
--sau đó sinh ra một bảng mới chứa các cột review_overall_bin, beer_beerID

A2 = FOREACH filtered_data_limit GENERATE (float) review_overall as review_overall_bin;

grouped_data = FOREACH (
    GROUP A2 BY (
        CASE
            WHEN (review_overall_bin >= 0 AND review_overall_bin < 1) THEN '[0 - 1]'
            WHEN (review_overall_bin >= 1 AND review_overall_bin < 2) THEN '[1 - 2]'
            WHEN (review_overall_bin >= 2 AND review_overall_bin < 3) THEN '[2 - 3]'
            WHEN (review_overall_bin >= 3 AND review_overall_bin < 4) THEN '[3 - 4]'
            WHEN (review_overall_bin >= 4 AND review_overall_bin <= 5) THEN '[4 - 5]'
            ELSE 'Unknown'
        END
    )
) GENERATE group AS range, COUNT(A2) AS count;

--Lưu trữ kết quả
STORE grouped_data into 'hdfs://hadoop-master:9820/beer/pig/output/ex2/' using PigStorage(',');