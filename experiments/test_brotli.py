import asyncio
import logging
import aiohttp
from services.ingestion_service.src.infrastructure.flashscore_provider import FlashscoreProvider

logging.basicConfig(level=logging.INFO)

async def run_brotli_test() -> None:
    print("Initializing FlashscoreProvider to extract the working security headers...")
    provider = FlashscoreProvider()
    
    # We call connect() to generate the session with all required WAF-bypass headers
    await provider.connect()

    # Match ID from your working test
    match_id = "0UwSV7kt" 
    url_stats = f"{provider.base_url}/df_st_1_{match_id}"

    # Extract the proven headers and proxy URL from your working provider
    # This guarantees we won't get a 403 Forbidden or 0 bytes
    working_headers = dict(provider.session._default_headers)
    proxy_url = provider.proxy_url

    # Close the provider's default session as we need a custom one for the byte measurement
    await provider.disconnect() 

    print(f"\nTesting compression for URL: {url_stats}")
    print(f"Using proxy configuration extracted from Provider.")

    # Create a special session that DOES NOT decompress automatically
    # This allows us to count the exact bytes traveling through the proxy tunnel
    async with aiohttp.ClientSession(auto_decompress=False) as session:
        
        # --- TEST 1: NO COMPRESSION ---
        headers_raw = working_headers.copy()
        headers_raw["Accept-Encoding"] = "identity" # Force the server to send raw, uncompressed text

        print("\nFetching without compression (Accept-Encoding: identity)...")
        async with session.get(url_stats, headers=headers_raw, proxy=proxy_url) as resp_raw:
            bytes_raw = await resp_raw.read()
            size_raw = len(bytes_raw)
            print(f"🔴 Raw size - HTTP Status {resp_raw.status}: {size_raw} bytes")

        # --- TEST 2: BROTLI COMPRESSION ---
        headers_br = working_headers.copy()
        headers_br["Accept-Encoding"] = "gzip, deflate, br" # Ask for Brotli compression

        print("\nFetching with compression (Accept-Encoding: gzip, deflate, br)...")
        async with session.get(url_stats, headers=headers_br, proxy=proxy_url) as resp_br:
            bytes_br = await resp_br.read()
            size_br = len(bytes_br)
            encoding_used = resp_br.headers.get("Content-Encoding", "None")
            print(f"🟢 Compressed size - HTTP Status {resp_br.status}: {size_br} bytes (Algorithm: {encoding_used})")

        # --- CALCULATE SAVINGS ---
        if size_raw > 0:
            saved = 100 - ((size_br / size_raw) * 100)
            print(f"\n🔥 Total bandwidth saved per request: {saved:.2f}%")
        else:
            print("\n❌ Failed to fetch data. Both sizes are 0. The match might be over or proxy failed.")

if __name__ == "__main__":
    try:
        asyncio.run(run_brotli_test())
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")