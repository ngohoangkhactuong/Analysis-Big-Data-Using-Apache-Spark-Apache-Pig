--Phân tích từng loại beer được người dùng đánh giá tổng quát cao nhất 
--và nồng độ cồn người dùng thích

--Lấy dữ liệu gốc từ file csv và tạo bảng:

raw_data = LOAD  'hdfs://hadoop-master:9820/beer/Beer.csv' using PigStorage(',');

structured_data = FOREACH raw_data GENERATE (float) $0 as beer_ABV, (int) $1 as beer_beerID, (int) $2 as beer_brewerID, $3 as beer_name, $4 as beer_style, (float) $5 as review_appearance, (float) $6 as review_palette, (float) $7 as review_overall, (float) $8 as review_taste, $9 as review_profileName;

--Lọc dữ liệu chọn và chọn 100000 dòng đầu tiên trong bảng:

filtered_data = FILTER structured_data BY review_overall is not null;
filtered_data = FILTER structured_data BY beer_ABV is not null;
filtered_data = FILTER structured_data BY beer_style is not null;

--Duyệt các thành phần trong filtered_data_limit sau đó sinh ra một bảng mới chứa các cột beer_ABV, beer_style, review_overall
-- và nhóm lại bảng theo beer_style. đối với mỗi nhóm (beer_style), tìm dòng có tổng thể được đánh giá cao nhất, sắp xếp theo thứ tự giảm dần. 

A1 = FOREACH filtered_data GENERATE beer_ABV as beer_ABV, beer_style as beer_style, review_overall as review_overall;
B1 = GROUP A1 by beer_style;
C1 = FOREACH B1 {
         I1 = ORDER A1 BY review_overall DESC;
         J1 = LIMIT I1 5;
         GENERATE group, J1.( beer_ABV, beer_style, review_overall);
};

--Lưu trữ kết quả:
STORE C1 into 'hdfs://hadoop-master:9820/beer/pig/output/ex1/' using PigStorage(',');