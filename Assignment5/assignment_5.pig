raw_data = LOAD '/user/duynguyen/input/hotel-review.csv' USING PigStorage(';') AS (
    id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray
);
stop_words = LOAD '/user/duynguyen/input/stopwords.txt' AS (stopword:chararray);

-- Tách từ và giữ lại Category 
all_tokens = FOREACH raw_data GENERATE category, FLATTEN(TOKENIZE(LOWER(review))) AS word;

-- Lọc stopword 
joined_data = JOIN all_tokens BY word LEFT OUTER, stop_words BY stopword;
clean_data = FILTER joined_data BY stop_words::stopword IS NULL;

-- Nhóm theo (Category, Word) để đếm tần suất
word_groups = GROUP clean_data BY (all_tokens::category, all_tokens::word);
word_counts = FOREACH word_groups GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(clean_data) AS freq;

-- 5. Lấy Top 5 từ cho mỗi Category 
cat_groups = GROUP word_counts BY category;
top5_related = FOREACH cat_groups {
    sorted = ORDER word_counts BY freq DESC;
    top_res = LIMIT sorted 5;
    GENERATE FLATTEN(top_res);
};

-- 6. Lưu kết quả
STORE top5_related INTO '/user/duynguyen/output/assign5_top5_related';