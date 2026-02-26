# Panora API Rate Limiting Guide

## Câu hỏi: Có bị hit rate limit nếu call API quá nhiều không?

**Câu trả lời: CÓ** - Hầu hết các API đều có rate limiting. Panora API cũng không ngoại lệ.

---

## Rate Limit Điều kiện

Dựa trên tiêu chuẩn API tiêu chuẩn:

- **429 Too Many Requests** - Bạn đã vượt quá limit
- **503 Service Unavailable** - Server quá tải

### Khuyến nghị an toàn

| Scenario | Interval | Safety | Status |
|----------|----------|--------|--------|
| **1 request/sec** | 1.0s | ✅ SAFE | Default (current) |
| **5 requests/sec** | 0.2s | ⚠️ RISKY | Có thể hit limit |
| **10 requests/sec** | 0.1s | ❌ DANGER | Chắc chắn hit limit |

---

## Cải tiến hiện tại (trong panora.py)

### 1. **Rate Limit Detection**
```python
if resp.status in (429, 503):
    self.rate_limited = True
    # Tự động retry với exponential backoff
```

### 2. **Exponential Backoff Retry**
```
Attempt 1: Chờ 1.0s
Attempt 2: Chờ 2.0s  
Attempt 3: Chờ 4.0s
```

### 3. **Configurable Parameters**
```python
client = PanoraClient(
    max_retries=3,           # Số lần retry
    base_retry_delay=1.0     # Delay đầu tiên (giây)
)
```

---

## Tình huống thực tế

### ✅ AN TOÀN - Polling 1 request/second
```python
# config/settings.py
panora_poll_interval = 1.0  # 1 giây giữa các request

# Khoảng 86,000 requests/ngày
# Phù hợp với free tier của hầu hết API
```

### ⚠️ NGUY HIỂM - Polling quá nhanh
```python
panora_poll_interval = 0.1  # 100 ms giữa các request

# Khoảng 860,000 requests/ngày  
# CHẮC CHẮN sẽ hit rate limit
```

---

## Logs khi bị Rate Limited

```
2026-02-24 14:01:00.123 | WARNING  | exchanges.panora:get_swap_quote:63 | 
Panora API rate limited (HTTP 429) - attempt 1/3

2026-02-24 14:01:00.123 | INFO     | exchanges.panora:get_swap_quote:70 | 
Retrying after 1.0s...

2026-02-24 14:01:01.124 | WARNING  | exchanges.panora:get_swap_quote:63 | 
Panora API rate limited (HTTP 429) - attempt 2/3

2026-02-24 14:01:01.124 | INFO     | exchanges.panora:get_swap_quote:70 | 
Retrying after 2.0s...

2026-02-24 14:01:03.125 | SUCCESS  | ... | Request succeeded on retry!
```

---

## Khuyến nghị Best Practices

### 1. **Luôn sử dụng rate limiting**
```python
# ✅ GOOD - Cân nhắc tần suất request
panora_poll_interval = 1.0  # Max 1 request/sec

# ❌ BAD - Request liên tục không kiểm soát
panora_poll_interval = 0.01  # 100 requests/sec → BẮT CHẮC hit limit
```

### 2. **Monitor rate limit status**
```python
if client.rate_limited:
    logger.warning("Panora API bị rate limit - kích hoạt backoff strategy")
    # Có thể tạm dừng poll hoặc tăng delay
```

### 3. **Cấu hình retry**
```python
# Tăng max_retries nếu server thường xuyên busy
client = PanoraClient(max_retries=5, base_retry_delay=2.0)

# Giảm nếu muốn fail nhanh hơn
client = PanoraClient(max_retries=1, base_retry_delay=0.5)
```

---

## So sánh trước/sau

| Feature | Before | After |
|---------|--------|-------|
| Rate limit handling | ❌ None | ✅ Auto-retry |
| 429/503 error | ❌ Log + fail | ✅ Exponential backoff |
| Retries | ❌ 0 | ✅ Up to 3 (configurable) |
| Timing aware | ❌ No | ✅ Yes (`rate_limited` flag) |

---

## Kiểm tra hiện tại

File đã được cập nhật: [exchanges/panora.py](exchanges/panora.py)

**Thay đổi:**
- ✅ Thêm retry logic
- ✅ Thêm rate limit detection (429, 503)  
- ✅ Thêm exponential backoff
- ✅ Thêm timeout handling
- ✅ Thêm configurable parameters

**Sử dụng:**
```bash
python3 test_arbitrage_scenario.py
```
