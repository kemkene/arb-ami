"""
Rate Limit Analysis: MEXC vs Bybit vs Panora
"""

# Current polling intervals
BYBIT_METHOD = "WebSocket (real-time)"  # No rate limit concerns
MEXC_INTERVAL = 0.2  # 200ms
PANORA_INTERVAL = 1.0  # 1000ms
ARB_CHECK_INTERVAL = 0.1  # Local check, no API call

# Calculate request rates
MEXC_REQUESTS_PER_SEC = 1 / MEXC_INTERVAL
MEXC_REQUESTS_PER_MIN = MEXC_REQUESTS_PER_SEC * 60
MEXC_REQUESTS_PER_HOUR = MEXC_REQUESTS_PER_MIN * 60

PANORA_REQUESTS_PER_SEC = 1 / PANORA_INTERVAL
PANORA_REQUESTS_PER_MIN = PANORA_REQUESTS_PER_SEC * 60
PANORA_REQUESTS_PER_HOUR = PANORA_REQUESTS_PER_MIN * 60

print("=" * 80)
print("RATE LIMIT ANALYSIS - Current Configuration")
print("=" * 80)

print("\n[BYBIT] - WebSocket Subscription")
print("-" * 80)
print(f"Method: {BYBIT_METHOD}")
print(f"⚠️ Rate Limit: NONE (WebSocket is event-driven, not polling)")
print(f"Status: ✅ SAFE")

print("\n[MEXC] - REST API (bookTicker)")
print("-" * 80)
print(f"Poll Interval: {MEXC_INTERVAL}s")
print(f"Requests/sec: {MEXC_REQUESTS_PER_SEC:.1f}")
print(f"Requests/min: {int(MEXC_REQUESTS_PER_MIN)}")
print(f"Requests/hour: {int(MEXC_REQUESTS_PER_HOUR)}")
print(f"\n⚠️ Rate Limit (Typical): 300-1200 requests/minute")
print(f"Current Usage: {int(MEXC_REQUESTS_PER_MIN)} requests/minute")
print(f"Usage %: {(MEXC_REQUESTS_PER_MIN / 300) * 100:.1f}% of typical free tier limit")

if MEXC_REQUESTS_PER_MIN >= 300:
    print(f"\n❌ STATUS: AT RISK - Very close to or exceeding free tier limit!")
else:
    print(f"\n✅ STATUS: SAFE - Within typical limits")

print("\n[PANORA] - REST API (Swap Quote)")
print("-" * 80)
print(f"Poll Interval: {PANORA_INTERVAL}s")
print(f"Requests/sec: {PANORA_REQUESTS_PER_SEC:.1f}")
print(f"Requests/min: {int(PANORA_REQUESTS_PER_MIN)}")
print(f"Requests/hour: {int(PANORA_REQUESTS_PER_HOUR)}")
print(f"\n⚠️ Rate Limit (Typical): 100-300 requests/minute (for free tier)")
print(f"Current Usage: {int(PANORA_REQUESTS_PER_MIN)} requests/minute")
print(f"Usage %: {(PANORA_REQUESTS_PER_MIN / 100) * 100:.1f}% of typical free tier limit")

if PANORA_REQUESTS_PER_MIN >= 100:
    print(f"\n⚠️ STATUS: OK - Within typical limits (but check your API tier)")
else:
    print(f"\n✅ STATUS: SAFE")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print("\n📊 API Call Distribution:")
print(f"  Bybit:  WebSocket (no REST calls) ✅")
print(f"  MEXC:   {int(MEXC_REQUESTS_PER_MIN)} req/min REST API ⚠️ HIGH")
print(f"  Panora: {int(PANORA_REQUESTS_PER_MIN)} req/min REST API ✅")

print("\n⚠️ RISK ASSESSMENT:")
print("\n1. MEXC at {:.1f}% of typical limit".format((MEXC_REQUESTS_PER_MIN / 300) * 100))
if MEXC_REQUESTS_PER_MIN >= 300:
    print("   → CRITICAL: Already at/exceeding limit, can get 429 errors")
    print("   → Recommend: Increase poll_interval from 0.2s to 1.0s or higher")
elif MEXC_REQUESTS_PER_MIN >= 240:
    print("   → WARNING: Very close to limit, may hit on spikes")
    print("   → Recommend: Increase poll_interval from 0.2s to 0.5s")
else:
    print("   → OK: Some buffer remaining")

print("\n2. Panora - Already has retry logic ✅")
print("   → Will auto-retry on 429/503 with exponential backoff")

print("\n" + "=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)

print("\n🔧 Option 1: Reduce MEXC Polling (Safest)")
print("   Change in .env or config:")
print("   ├ MEXC_POLL_INTERVAL=0.5    → 120 requests/min (Safe)")
print("   └ or MEXC_POLL_INTERVAL=1.0 → 60 requests/min (Very Safe)")

print("\n🔧 Option 2: Check MEXC API Tier")
print("   ├ Free tier: usually 300 req/min")
print("   ├ Pro tier: usually 1000+ req/min")
print("   └ If you have pro tier, current 0.2s is OK")

print("\n🔧 Option 3: Add Retry Logic to MEXC (Like Panora)")
print("   └ Already partially done (error handling exists)")

print("\n" + "=" * 80)

# Create suggestions for each case
print("\nRECOMMENDED CONFIG based on tier:")
print("-" * 80)
print("\n[CASE A] Free tier MEXC")
print("  MEXC_POLL_INTERVAL=1.0     # 60 requests/min - comfortable buffer")
print("  PANORA_POLL_INTERVAL=1.0   # 60 requests/min - default")

print("\n[CASE B] Standard tier MEXC (500-1000 req/min)")
print("  MEXC_POLL_INTERVAL=0.3     # 200 requests/min - safe")
print("  PANORA_POLL_INTERVAL=1.0   # 60 requests/min - default")

print("\n[CASE C] Enterprise tier MEXC (2000+ req/min)")
print("  MEXC_POLL_INTERVAL=0.2     # 300 requests/min - current (OK)")
print("  PANORA_POLL_INTERVAL=0.5   # 120 requests/min - higher frequency")

print("\n" + "=" * 80)
