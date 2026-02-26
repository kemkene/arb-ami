"""Unit tests for PanoraClient and PanoraPoller."""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
import asyncio

from exchanges.panora import PanoraClient
from exchanges.panora_poller import PanoraPoller
from core.price_collector import PriceCollector


# ------------------------------------------------------------------ #
#  PanoraClient unit tests
# ------------------------------------------------------------------ #
class TestPanoraClient(unittest.TestCase):
    """Synchronous tests for pure helper methods."""

    def test_parse_to_token_amount_from_top_level(self):
        client = PanoraClient()
        data = {"toTokenAmount": "0.008041"}
        self.assertAlmostEqual(client.parse_to_token_amount(data), 0.008041)

    def test_parse_to_token_amount_from_quotes(self):
        client = PanoraClient()
        data = {"quotes": [{"toTokenAmount": "1.234"}]}
        self.assertAlmostEqual(client.parse_to_token_amount(data), 1.234)

    def test_parse_to_token_amount_empty_quotes(self):
        client = PanoraClient()
        self.assertIsNone(client.parse_to_token_amount({"quotes": []}))
        self.assertIsNone(client.parse_to_token_amount({}))

    def test_parse_to_token_amount_invalid_value(self):
        client = PanoraClient()
        self.assertIsNone(client.parse_to_token_amount({"toTokenAmount": "abc"}))

    def test_parse_from_token_amount_from_top_level(self):
        client = PanoraClient()
        data = {"fromTokenAmount": "10.5"}
        self.assertAlmostEqual(client.parse_from_token_amount(data), 10.5)

    def test_parse_from_token_amount_from_quotes(self):
        client = PanoraClient()
        data = {"quotes": [{"fromTokenAmount": "99.9"}]}
        self.assertAlmostEqual(client.parse_from_token_amount(data), 99.9)

    def test_parse_from_token_amount_missing(self):
        client = PanoraClient()
        self.assertIsNone(client.parse_from_token_amount({}))

    def test_rate_limit_stats_initial(self):
        client = PanoraClient()
        stats = client.rate_limit_stats()
        self.assertIn("requests=0", stats)
        self.assertIn("rate_limits=0", stats)
        self.assertIn("currently_limited=False", stats)

    def test_rate_limit_tracking(self):
        client = PanoraClient()
        client._total_requests = 100
        client._total_rate_limits = 5
        client.rate_limited = True
        stats = client.rate_limit_stats()
        self.assertIn("requests=100", stats)
        self.assertIn("rate_limits=5", stats)
        self.assertIn("5.0%", stats)
        self.assertIn("currently_limited=True", stats)

    def test_default_addresses(self):
        client = PanoraClient()
        self.assertTrue(client.from_token_address.startswith("0x"))
        self.assertTrue(client.to_token_address.startswith("0x"))

    def test_custom_addresses(self):
        client = PanoraClient(
            from_token_address="0xabc",
            to_token_address="0xdef",
        )
        self.assertEqual(client.from_token_address, "0xabc")
        self.assertEqual(client.to_token_address, "0xdef")


class TestPanoraClientAsync(unittest.IsolatedAsyncioTestCase):
    """Async tests for API-calling methods (mocked network)."""

    def _mock_client_with_response(self, status, json_data=None, text_data="", headers=None):
        """Create a PanoraClient with a mocked session returning the given response."""
        client = PanoraClient(max_retries=1, base_retry_delay=0.01)

        mock_resp = AsyncMock()
        mock_resp.status = status
        if json_data is not None:
            mock_resp.json = AsyncMock(return_value=json_data)
        mock_resp.text = AsyncMock(return_value=text_data)
        mock_resp.headers = headers or {}

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_cm)
        mock_session.closed = False
        mock_session.close = AsyncMock()

        client._session = mock_session
        # Prevent _get_session from creating a real session
        client._get_session = AsyncMock(return_value=mock_session)

        return client, mock_resp

    async def test_get_swap_quote_success(self):
        mock_response = {"quotes": [{"toTokenAmount": "0.008041"}]}
        client, _ = self._mock_client_with_response(200, json_data=mock_response)

        result = await client.get_swap_quote(1.0)
        self.assertIsNotNone(result)
        self.assertEqual(result, mock_response)

    async def test_get_swap_quote_rate_limited(self):
        client, _ = self._mock_client_with_response(
            429, text_data='{"error":"too many requests"}'
        )

        result = await client.get_swap_quote(1.0)
        self.assertIsNone(result)
        self.assertTrue(client.rate_limited)
        self.assertEqual(client._total_rate_limits, 1)

    async def test_get_swap_quote_http_error(self):
        client, _ = self._mock_client_with_response(
            500, text_data="Internal Server Error"
        )

        result = await client.get_swap_quote(1.0)
        self.assertIsNone(result)

    async def test_get_price_returns_tuple(self):
        mock_response = {"toTokenAmount": "0.008041"}
        client, _ = self._mock_client_with_response(200, json_data=mock_response)

        result = await client.get_price(1.0)
        self.assertIsNotNone(result)
        bid, ask = result
        self.assertAlmostEqual(bid, 0.008041)
        self.assertEqual(bid, ask)  # DEX: bid == ask

    async def test_get_price_returns_none_on_failure(self):
        client, _ = self._mock_client_with_response(500, text_data="error")

        result = await client.get_price(1.0)
        self.assertIsNone(result)

    async def test_get_price_zero_amount_returns_none(self):
        mock_response = {"toTokenAmount": "0"}
        client, _ = self._mock_client_with_response(200, json_data=mock_response)

        result = await client.get_price(1.0)
        self.assertIsNone(result)

    async def test_close(self):
        client = PanoraClient()
        mock_session = AsyncMock()
        mock_session.closed = False
        client._session = mock_session

        await client.close()
        mock_session.close.assert_called_once()
        self.assertIsNone(client._session)

    async def test_retry_on_429_with_retry_after(self):
        """Test that Retry-After header is respected."""
        client = PanoraClient(max_retries=2, base_retry_delay=0.01)

        mock_resp_429 = AsyncMock()
        mock_resp_429.status = 429
        mock_resp_429.text = AsyncMock(return_value="rate limited")
        mock_resp_429.headers = {"Retry-After": "1"}

        mock_resp_200 = AsyncMock()
        mock_resp_200.status = 200
        mock_resp_200.json = AsyncMock(return_value={"toTokenAmount": "0.01"})

        call_count = 0

        async def fake_aenter(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_resp_429
            return mock_resp_200

        mock_cm = MagicMock()
        mock_cm.__aenter__ = fake_aenter
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_cm)
        mock_session.closed = False

        client._session = mock_session
        client._get_session = AsyncMock(return_value=mock_session)

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await client.get_swap_quote(1.0)

        self.assertIsNotNone(result)
        mock_sleep.assert_called_once_with(1)


# ------------------------------------------------------------------ #
#  PanoraPoller unit tests
# ------------------------------------------------------------------ #
class TestPanoraPoller(unittest.TestCase):

    def test_symbol_format(self):
        collector = PriceCollector()
        poller = PanoraPoller(collector, from_amount=1.0)
        # Symbol should be first 4 chars of each address
        self.assertTrue(poller.symbol.startswith("0xb3"))
        self.assertIn("_", poller.symbol)

    def test_custom_addresses(self):
        collector = PriceCollector()
        poller = PanoraPoller(
            collector,
            from_token_address="0xAAAA1234",
            to_token_address="0xBBBB5678",
        )
        self.assertEqual(poller.symbol, "0xAA_0xBB")


if __name__ == "__main__":
    unittest.main()
