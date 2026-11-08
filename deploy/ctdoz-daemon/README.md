# ctdoz-daemon: bot Zalo chạy headless (Docker)

Phần custom **không sửa file nào của upstream**:

| Đường dẫn | Nội dung |
|---|---|
| `src/bin/ctdoz-daemon/` | binary riêng; Cargo tự nhận, chỉ dùng API public của thư viện `zalo_rust` |
| `deploy/ctdoz-daemon/` | Dockerfile, compose, cấu hình mẫu, script đồng bộ upstream |

Đồng bộ upstream: `deploy/ctdoz-daemon/sync-upstream.sh`. Script merge `upstream/main`, rồi build và test daemon.
Nếu upstream đổi hàm public mà daemon dùng (`chat::send_message`, `qr::*`, `session::*`, `store::*`, `scheduler::*`, `api_mcp::*`), lỗi sẽ hiện lúc compile, không lẻn vào lúc chạy.

## Chức năng

- Nhận tin đến qua WebSocket, có polling dự phòng, rồi lưu vào `zalo_history.db`.
- Tin khớp trigger trong `automation.json` (keyword, @mention, nhóm cụ thể, hoặc tất cả) được POST tới webhook AI của bạn. Webhook trả `{"reply": "..."}` thì daemon gửi lại câu đó vào Zalo.
- Chạy auto-reply theo keyword tĩnh (`autoreply_rules.json`) và gửi tin hẹn giờ.
- REST API và MCP ở `127.0.0.1:17890`, yêu cầu Bearer token `CTDOZLITE_API_TOKEN`.
- **Cảnh báo Telegram** (cùng loại lỗi chỉ báo tối đa 1 lần mỗi 15 phút) khi:
  - webhook lỗi hoặc timeout,
  - gửi tin thất bại,
  - polling lỗi liên tục, WebSocket đóng, REST API dừng,
  - daemon crash hoặc panic,
  - không kết nối được Zalo.
- **Phiên hết hạn:** daemon tự tạo mã QR mới và gửi ảnh QR qua Telegram. Quét bằng app Zalo là chạy tiếp, không cần SSH.

## Triển khai

```bash
cd deploy/ctdoz-daemon
cp .env.example .env && chmod 600 .env        # điền TELEGRAM_*, CTDOZLITE_API_TOKEN, CTDOZLITE_WEBHOOK_SECRET
install -d -m700 data config && cp config/automation.example.json config/automation.json
sudo chown 10001:10001 data
docker compose up -d --build
./unlock.sh                                    # nhập mật khẩu vault (lần đầu: đặt mật khẩu mới)
docker compose logs -f                         # lần đầu: QR hiện trong log và được gửi qua Telegram
```

Bot Telegram: tạo bằng @BotFather, nhắn cho bot một tin, rồi lấy `chat.id` từ
`https://api.telegram.org/bot<TOKEN>/getUpdates`.

## Mã hoá dữ liệu (vault)

- Thư viện vẫn đọc/ghi file thường (`session.json`, `zalo_history.db`…), nhưng các file đó nằm trong **tmpfs `/work` (chỉ ở RAM)**.
- Thứ duy nhất ghi xuống đĩa là `data/vault.enc`, mã hoá **AES-256-GCM**. Daemon ghi lại file này mỗi 60 giây nếu dữ liệu có thay đổi, khi đăng nhập xong, và khi container dừng (`docker compose stop`).
  - SQLite được chụp bằng `VACUUM INTO` nên bản chụp luôn nhất quán.
  - Các thư mục `*cache*` không được lưu, vì tải lại được.
- **Khoá sinh từ mật khẩu bạn tự nhập:** PBKDF2-HMAC-SHA256, 600.000 vòng, salt ngẫu nhiên lưu trong header của vault. Header cũng được xác thực bằng GCM nên không sửa được.
  - Mật khẩu không bao giờ nằm trên đĩa: `./unlock.sh` hỏi mật khẩu (không hiện ra màn hình) rồi đặt vào `/run/ctdoz/passphrase`, là tmpfs của host. Daemon đọc xong xoá ngay.
  - Lần đầu, `unlock.sh` bắt nhập lại để xác nhận; mật khẩu tối thiểu 8 ký tự (khuyên dùng ≥12, hoặc 8 ký tự ngẫu nhiên trộn hoa/thường/số/ký tự đặc biệt).
- **Sau mỗi lần container hoặc server khởi động lại**, daemon ở trạng thái khoá và báo Telegram 🔒. Bạn SSH vào và chạy `./unlock.sh`. Nhập sai mật khẩu thì có báo ⛔ và daemon tiếp tục chờ.
- **Bảo vệ được:** đĩa, snapshot EBS hoặc backup bị lộ; container khác đọc trộm thư mục.
- **Không bảo vệ được:** ai có root trên server trong lúc daemon đang chạy, vì người đó đọc được RAM và tmpfs.
- **Mất mật khẩu thì không giải mã được vault:** mất lịch sử chat và phải quét QR lại. Hãy cất mật khẩu trong trình quản lý mật khẩu.
- Khi daemon bị kill đột ngột (OOM, mất điện), tối đa 60 giây dữ liệu cuối chưa được ghi vào vault.
