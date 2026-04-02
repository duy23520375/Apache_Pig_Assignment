raw_data = LOAD '/user/duynguyen/input/hotel-review.csv' USING PigStorage(';') AS (
    id:int, 
    review:chararray, 
    category:chararray, 
    aspect:chararray, 
    sentiment:chararray
);

-- YÊU CẦU 1: Khía cạnh TÍCH CỰC (positive) nhất
pos_data = FILTER raw_data BY sentiment == 'positive';
pos_groups = GROUP pos_data BY aspect;
pos_counts = FOREACH pos_groups GENERATE group AS aspect, COUNT(pos_data) AS num_pos;

pos_bag = GROUP pos_counts ALL;
top_pos_result = FOREACH pos_bag {
    result = TOP(1, 1, pos_counts); 
    GENERATE FLATTEN(result);
};

STORE top_pos_result INTO '/user/duynguyen/output/assign3_top_positive';

-- YÊU CẦU 2: Khía cạnh TIÊU CỰC (negative) nhất
neg_data = FILTER raw_data BY sentiment == 'negative';
neg_groups = GROUP neg_data BY aspect;
neg_counts = FOREACH neg_groups GENERATE group AS aspect, COUNT(neg_data) AS num_neg;

neg_bag = GROUP neg_counts ALL;
top_neg_result = FOREACH neg_bag {
    result = TOP(1, 1, neg_counts);
    GENERATE FLATTEN(result);
};

STORE top_neg_result INTO '/user/duynguyen/output/assign3_top_negative';