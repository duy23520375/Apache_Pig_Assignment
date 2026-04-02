-- 1. Load dữ liệu từ HDFS 
raw_data = LOAD '/user/duynguyen/input/hotel-review.csv' USING PigStorage(';') AS (
    id:int, 
    review:chararray, 
    category:chararray, 
    aspect:chararray, 
    sentiment:chararray
);

-- 2. Load danh sách stopword
stop_words = LOAD '/user/duynguyen/input/stopwords.txt' AS (stopword:chararray);

-- 3. Xử lý: Viết thường và Tách từ theo khoảng trắng
tokens = FOREACH raw_data GENERATE id, FLATTEN(TOKENIZE(LOWER(review))) AS word;

-- 4. Loại bỏ Stopwords bằng JOIN 
joined_data = JOIN tokens BY word LEFT OUTER, stop_words BY stopword;
filtered_tokens = FILTER joined_data BY stop_words::stopword IS NULL;

-- 5. Kết quả 
final_output = FOREACH filtered_tokens GENERATE tokens::id AS id, tokens::word AS word;

-- 6. Lưu kết quả ra HDFS
STORE final_output INTO '/user/duynguyen/output/result' USING PigStorage(',');