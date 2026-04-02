raw_data = LOAD '/user/duynguyen/input/hotel-review.csv' USING PigStorage(';') AS (
    id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray
);
stop_words = LOAD '/user/duynguyen/input/stopwords.txt' AS (stopword:chararray);

-- YÊU CẦU 1: 5 từ TÍCH CỰC nhất theo từng Category
pos_data = FILTER raw_data BY sentiment == 'positive';

-- Tách từ và giữ lại Category
pos_tokens = FOREACH pos_data GENERATE category, FLATTEN(TOKENIZE(LOWER(review))) AS word;

-- Lọc stopword
pos_joined = JOIN pos_tokens BY word LEFT OUTER, stop_words BY stopword;
pos_clean = FILTER pos_joined BY stop_words::stopword IS NULL;

-- Nhóm theo (Category, Word) để đếm tần suất của từ trong Category đó
pos_word_groups = GROUP pos_clean BY (pos_tokens::category, pos_tokens::word);
pos_word_counts = FOREACH pos_word_groups GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(pos_clean) AS freq;

-- Nhóm theo Category để lấy Top 5 từ của mỗi Group
pos_cat_group = GROUP pos_word_counts BY category;
top5_pos = FOREACH pos_cat_group {
    sorted = ORDER pos_word_counts BY freq DESC;
    top_result = LIMIT sorted 5;
    GENERATE FLATTEN(top_result);
};

STORE top5_pos INTO '/user/duynguyen/output/assign4_top5_positive';

--------------------------------------------------------------
-- YÊU CẦU 2: 5 từ TIÊU CỰC nhất theo từng Category
--------------------------------------------------------------
neg_data = FILTER raw_data BY sentiment == 'negative';
neg_tokens = FOREACH neg_data GENERATE category, FLATTEN(TOKENIZE(LOWER(review))) AS word;

neg_joined = JOIN neg_tokens BY word LEFT OUTER, stop_words BY stopword;
neg_clean = FILTER neg_joined BY stop_words::stopword IS NULL;

neg_word_groups = GROUP neg_clean BY (neg_tokens::category, neg_tokens::word);
neg_word_counts = FOREACH neg_word_groups GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(neg_clean) AS freq;

neg_cat_group = GROUP neg_word_counts BY category;
top5_neg = FOREACH neg_cat_group {
    sorted = ORDER neg_word_counts BY freq DESC;
    top_result = LIMIT sorted 5;
    GENERATE FLATTEN(top_result);
};

STORE top5_neg INTO '/user/duynguyen/output/assign4_top5_negative';