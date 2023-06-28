--Đếm từng loại beer style có bao nhiêu beer khác nhau

--Lấy dữ liệu gốc từ file csv và tạo bảng:

raw_data = LOAD  'hdfs://hadoop-master:9820/beer/Beer.csv' using PigStorage(',');

structured_data = FOREACH raw_data GENERATE (float) $0 as beer_ABV, (int) $1 as beer_beerID, (int) $2 as beer_brewerID, $3 as beer_name, $4 as beer_style, (float) $5 as review_appearance, (float) $6 as review_palette, (float) $7 as review_overall, (float) $8 as review_taste, $9 as review_profileName;

--Lọc dữ liệu chọn và chọn 100000 dòng đầu tiên trong bảng:

filtered_data = FILTER structured_data BY review_overall is not null;
filtered_data = FILTER structured_data BY beer_beerID is not null;
filtered_data_limit = LIMIT filtered_data 100000;

--Duyệt các thành phần trong filtered_data_limit sau đó sinh ra một bảng mới chứa các cột beer_style có kiểu dữ liệu là chararray, beer_beerID

A3 = FOREACH filtered_data_limit GENERATE (chararray) beer_style as beer_style, beer_beerID as beer_beerID;

--group các dòng theo beer_style

B3 = GROUP A3 by beer_style;

--Với mỗi beer_style, đếm số lượng của beerID và chỉ lấy ra các beer_beerID có ID khác nhau

C3 = FOREACH B3 {

         beer_ids = DISTINCT A3. beer_beerID;

         GENERATE group, COUNT(beer_ids) as number_beer;

};

--Lưu trữ kết quả:
STORE C3 into 'hdfs://hadoop-master:9820/beer/pig/output/ex3/' using PigStorage(',');