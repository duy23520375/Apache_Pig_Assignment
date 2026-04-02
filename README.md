# Big Data with Apache Pig

## Giới thiệu chung
Bài tập thực hiện phân tích tập dữ liệu bình luận khách sạn (`hotel-review.csv`) bằng ngôn ngữ **Pig Latin** trên nền tảng **Hadoop MapReduce**. 

---

## Nội dung các Assignment

### Bài 1: Tiền xử lý dữ liệu (Data Preprocessing)
- **Mục tiêu:** Làm sạch dữ liệu thô.
- **Nội dung:** Chuyển đổi toàn bộ bình luận sang chữ thường, thực hiện tách từ (Tokenization) và sử dụng danh sách `stopwords.txt` để loại bỏ các từ dừng không mang ý nghĩa phân tích.

### Bài 2: Thống kê tần suất (Statistics)
- **Mục tiêu:** Hiểu tổng quan về tập dữ liệu.
- **Nội dung:** - Thống kê các từ xuất hiện nhiều nhất (tần suất > 500 lần).
  - Thống kê số lượng bình luận theo từng Phân loại (Category).
  - Thống kê số lượng bình luận theo từng Khía cạnh (Aspect).

### Bài 3: Xác định Top Khía cạnh (Top Sentiment Aspect)
- **Mục tiêu:** Tìm ra điểm mạnh và điểm yếu nhất của khách sạn.
- **Nội dung:** Xác định khía cạnh nhận được nhiều đánh giá Tích cực (`positive`) nhất và khía cạnh nhận nhiều đánh giá Tiêu cực (`negative`) nhất.

### Bài 4: Phân tích từ khóa theo cảm xúc (Sentiment Keywords)
- **Mục tiêu:** Tìm các từ đặc trưng cho từng loại cảm xúc.
- **Nội dung:** Với mỗi Phân loại (Category), xác định 5 từ khóa xuất hiện nhiều nhất trong nhóm Tích cực và 5 từ khóa xuất hiện nhiều nhất trong nhóm Tiêu cực.

### Bài 5: Phân tích từ khóa liên quan (Related Keywords)
- **Mục tiêu:** Xác định các thực thể liên quan nhất theo chủ đề.
- **Nội dung:** Dựa trên từng Phân loại bình luận, xác định 5 từ có tần suất xuất hiện cao nhất để phác họa đặc điểm của phân loại đó.

