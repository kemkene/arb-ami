# Pool Reserves Bootstrap - API Request Reference

## REST API Call Structure

The bootstrap function queries the Cellana pool_reserves view function via the Aptos REST API.

### Request Details

**Endpoint:** `POST https://fullnode.mainnet.aptoslabs.com/v1/view`

**Request Body:**
```json
{
  "function": "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::pool_reserves",
  "type_arguments": [
    "0x1::object::ObjectCore",
    "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
    "0x1::aptos_coin::AptosCoin"
  ],
  "arguments": [
    "0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464"
  ]
}
```

### Parameter Breakdown

| Field | Value | Purpose |
|-------|-------|---------|
| `function` | `0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::pool_reserves` | Module and function to call |
| `type_arguments[0]` | `0x1::object::ObjectCore` | Type constraint for pool_id |
| `type_arguments[1]` | `0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451` | AMI token type (from settings.ami_token_address) |
| `type_arguments[2]` | `0x1::aptos_coin::AptosCoin` | APT token type (from settings.apt_token_address) |
| `arguments[0]` | `0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464` | AMI/APT pool address |

### Expected Response

```json
[
  "1000000",
  "10000000"
]
```

This is an array with two string values:
- `result[0]` = AMI reserves (as string) → convert to int
- `result[1]` = APT reserves (as string) → convert to int

### Python Implementation

```python
import requests
import json

rest_api_url = "https://fullnode.mainnet.aptoslabs.com/v1"

view_request = {
    "function": "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::pool_reserves",
    "type_arguments": [
        "0x1::object::ObjectCore",
        "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",  # AMI
        "0x1::aptos_coin::AptosCoin"  # APT
    ],
    "arguments": [
        "0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464"  # Pool ID
    ]
}

response = requests.post(f"{rest_api_url}/view", json=view_request, timeout=10)
result = response.json()

reserves_ami = int(result[0])
reserves_apt = int(result[1])

# Calculate price
price_ami_per_apt = reserves_apt / reserves_ami
print(f"AMI price: {price_ami_per_apt:.8f} APT per AMI")
```

## Verifying Against Explorer

To manually verify bootstrap is working correctly:

1. **Go to Aptos Explorer:**
   https://explorer.aptoslabs.com/account/0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1/modules/view/liquidity_pool/pool_reserves

2. **Fill in parameters:**
   - **pool_id (T0 generic for ObjectCore):** `0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464`
   - Keep type parameters as default or specify:
     - T0: `0x1::object::ObjectCore`
     - T1: AMI token
     - T2: APT token

3. **Click "View Function" to see current reserves**

4. **Compare with bot logs:**
   ```
   ✅ [BOOTSTRAP] AMI/APT Pool Initialized
      Reserves: AMI=XXXX | APT=YYYY
      Price (spot): Z.ZZZZZZZZ AMI/APT
   ```

## Debugging API Calls

### Using cURL

```bash
curl -X POST https://fullnode.mainnet.aptoslabs.com/v1/view \
  -H "Content-Type: application/json" \
  -d '{
    "function": "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::pool_reserves",
    "type_arguments": [
      "0x1::object::ObjectCore",
      "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
      "0x1::aptos_coin::AptosCoin"
    ],
    "arguments": ["0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464"]
  }'
```

### Using Python Directly

```python
import requests

response = requests.post(
    "https://fullnode.mainnet.aptoslabs.com/v1/view",
    json={
        "function": "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::pool_reserves",
        "type_arguments": [
            "0x1::object::ObjectCore",
            "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
            "0x1::aptos_coin::AptosCoin"
        ],
        "arguments": ["0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464"]
    }
)

print(json.dumps(response.json(), indent=2))
```

## Common Issues

### Issue: "Invalid type argument"
**Cause:** Wrong token address
**Solution:** Verify `ami_token_address` and `apt_token_address` in config/settings.py

### Issue: API returns error 400
**Cause:** Malformed request or wrong pool ID
**Solution:** Check pool_address is correct: `0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464`

### Issue: Bootstrap times out after 10 seconds
**Cause:** REST API slow or unreachable
**Solution:** Try manually with cURL first, check network connectivity

### Issue: Result is empty array or null
**Cause:** Pool doesn't exist or address is wrong
**Solution:** Double-check pool address on Aptos explorer

## Addresses Quick Reference

| Item | Address |
|------|---------|
| Cellana Module | `0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1` |
| Liquidity Pool Function | `::liquidity_pool::pool_reserves` |
| AMI Token (default) | `0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451` |
| APT Token | `0x1::aptos_coin::AptosCoin` |
| AMI/APT Pool ID | `0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464` |
| Object Core Type | `0x1::object::ObjectCore` |
| REST API Base | `https://fullnode.mainnet.aptoslabs.com/v1` |
