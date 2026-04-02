raw_data = LOAD '/user/duynguyen/input/hotel-review.csv' USING PigStorage(';') AS (
    id:int, 
    review:chararray, 
    category:chararray, 
    aspect:chararray, 
    sentiment:chararray
);
stop_words = LOAD '/user/duynguyen/input/stopwords.txt' AS (stopword:chararray);

-- YÊU CẦU 1: Thống kê tần suất từ (xuất hiện trên 500 lần)
tokens = FOREACH raw_data GENERATE FLATTEN(TOKENIZE(LOWER(review))) AS word;
joined_words = JOIN tokens BY word LEFT OUTER, stop_words BY stopword;
clean_tokens = FILTER joined_words BY stop_words::stopword IS NULL;

-- Nhóm theo từ và đếm
word_groups = GROUP clean_tokens BY tokens::word;
word_count = FOREACH word_groups GENERATE group AS word, COUNT(clean_tokens) AS frequency;

-- Lấy các từ xuất hiện trên 500 lần
popular_words = FILTER word_count BY frequency > 500;

STORE popular_words INTO '/user/duynguyen/output/assign2_word_count';

-- YÊU CẦU 2: Thống kê số bình luận theo từng phân loại (category)
distinct_reviews_cat = DISTINCT (FOREACH raw_data GENERATE id, category);
cat_groups = GROUP distinct_reviews_cat BY category;
cat_count = FOREACH cat_groups GENERATE group AS category, COUNT(distinct_reviews_cat) AS num_reviews;

STORE cat_count INTO '/user/duynguyen/output/assign2_category_count';

-- YÊU CẦU 3: Thống kê số bình luận theo từng khía cạnh (aspect)
distinct_reviews_asp = DISTINCT (FOREACH raw_data GENERATE id, aspect);
asp_groups = GROUP distinct_reviews_asp BY aspect;
asp_count = FOREACH asp_groups GENERATE group AS aspect, COUNT(distinct_reviews_asp) AS num_reviews;

STORE asp_count INTO '/user/duynguyen/output/assign2_aspect_count';