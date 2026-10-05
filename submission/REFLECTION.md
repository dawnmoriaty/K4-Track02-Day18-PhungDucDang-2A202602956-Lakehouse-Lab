# Lakehouse Reflection: Phòng ngừa Lifecycle Bug trong Hệ thống RAG

**Anti-pattern:** Tách rời vòng đời giữa Lakehouse và External Vector Index (Lifecycle Bug). Khi dữ liệu bị xóa trên Lakehouse (GDPR Right-to-be-forgotten), Vector DB ngoại vi vẫn giữ bản ghi cũ và trả về thông tin nhạy cảm cho RAG chatbot.

**Hệ thống quan tâm:** Trợ lý AI tra cứu tài liệu nội bộ doanh nghiệp. Khi tài liệu hết hạn hoặc nhân sự nghỉ việc, việc LLM vẫn truy xuất dữ liệu cũ gây rủi ro bảo mật và pháp lý nghiêm trọng.

**Giải pháp phòng tránh:**
1. Kích hoạt **Delta Change Data Feed (CDF)** để phát luồng sự kiện `delete`.
2. Xây dựng worker đồng bộ tự động tiêu thụ event để xóa vector trong Vector DB ngay khi commit hoàn tất.
3. Thiết lập job đối soát định kỳ (reconciliation) so khớp ID giữa Lakehouse và Vector DB.

**Khai báo AI:** Dùng Antigravity AI hỗ trợ rà soát luồng thực thi và định dạng báo cáo. Toàn bộ notebook do học viên tự chạy và lưu output thực tế trên máy.
