from homedepot import HomeDepot, NotFound
import os, csv, concurrent.futures, asyncio, aiohttp
from datetime import datetime
import json


async def main_async():
    homedepot = HomeDepot()

    products = homedepot.load_products('Reduced product List 2025 10 30.csv')
    stores = homedepot.load_stores()

    # Filter out products with invalid OMSID (critical since we use it for API calls)
    valid_products = []
    for product in products:
        omsid_val = product.get('omsid', '').strip()

        # Strict OMSID validation - disqualify on any of these invalid values
        if (omsid_val is None or
            omsid_val == '' or
            omsid_val.lower() in ['null', 'none', 'n/a', 'na'] or
            omsid_val == '0'):
            continue  # Skip this invalid product

        valid_products.append(product)

    print(f"Filtered {len(products)} to {len(valid_products)} valid products ({len(products) - len(valid_products)} invalid)")

    # Filter out stores with invalid/incorrect zipcodes
    valid_stores = []
    for store in stores:
        zipcode = store.get('zipcode', '').strip()
        if (zipcode and
            len(zipcode) >= 5 and
            zipcode.isdigit() and
            zipcode != '0'):
            valid_stores.append(store)

    print(f"Filtered {len(stores)} to {len(valid_stores)} valid stores ({len(stores) - len(valid_stores)} invalid)")

    test_store = valid_stores[0] if valid_stores else None

    # Calculate expected results (approximate, not all products may be available)
    total_expected = len(valid_products) * len(valid_stores)
    print(f"🚀 Estimated max results: {total_expected:,} combinations ({len(valid_products):,} products × {len(valid_stores)} stores)")
    print(f" Estimated runtime: {total_expected/100000:.1f} hours at 100K requests/hour")

    headers = ['name', 'brand', 'url', 'mainImageurl', 'SKU', 'Reviews', 'Rating', 'Model', 'retailer', 'storesku', 'omsid','storeName','storeID','storeLocation','inventory']
    csv_file = f'product-{datetime.now().strftime("%Y-%m-%d")}.csv'
    results_folder = os.path.join(homedepot.root_dir, 'results')

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    products = valid_products  # Now use valid_products directly
    stores = valid_stores    # Use filtered stores

    with open(f'{results_folder}/{csv_file}', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

    # Use async processing with semaphore for controlled concurrency
    semaphore = asyncio.Semaphore(200)  # Concurrent limit
    batch_size = 10000
    all_results = []

    async def process_combination(session, store, product, semaphore):
        async with semaphore:
            try:
                success, result = await homedepot.scan_items_async(session, store, product)
                if success:
                    return result['data']
                else:
                    print(f'Failed: {product["omsid"]} at {store["store_name"]}: {result["message"]}')
                    return None
                
            except Exception as e:
                print(f'Exception: {product["omsid"]} at {store["store_name"]}: {e}')
                return None

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=aiohttp.TCPConnector(limit=200)
    ) as session:
        write_lock = asyncio.Lock()
        product_semaphore = asyncio.Semaphore(10)  # Limit concurrent products

        async def process_product(product):
            async with product_semaphore:
                try:
                    success, result = await homedepot.scan_items_async(session, test_store, product)
                    if not success and 'not available' in result.lower():
                        raise NotFound(f"Initial test failed: {result}")
                    elif not success:
                        raise Exception(f"Initial test failed: {result}")
                    
                    # Product is available, now process all stores for this product
                    combinations = [(store, product) for store in stores]
                    print(f"Processing product {product['SKU']} - {len(combinations)} store combinations...")

                    for i in range(0, len(combinations), batch_size):
                        batch = combinations[i:i+batch_size]
                        print(f"  Batch {i//batch_size + 1}/{(len(combinations) + batch_size - 1)//batch_size} ({len(batch)} combinations)")

                        tasks = [
                            process_combination(session, store, product, semaphore)
                            for store, product in batch
                        ]

                        batch_results = await asyncio.gather(*tasks)
                        valid_results = [r for r in batch_results if r is not None]

                        async with write_lock:
                            with open(f'{results_folder}/{csv_file}', 'a', encoding='utf-8', newline='') as f:
                                writer = csv.writer(f)
                                for result in valid_results:
                                    writer.writerow(result.values())

                        all_results.extend(valid_results)
                        print(f"  Completed batch: {len(valid_results)} successful results")

                except NotFound:
                    print(f"Skipping product {product['SKU']} - not available")
                except Exception as e:
                    print(f"Error testing product {product['SKU']}: {e}")

        # Launch all product tasks concurrently (limited by semaphore)
        product_tasks = [process_product(product) for product in products]
        await asyncio.gather(*product_tasks)

    return len(all_results)


def main():
    print("Starting optimized Home Depot scraper...")
    total_processed = asyncio.run(main_async())
    print(f"Scraper completed. Processed {total_processed} product-store combinations.")


if __name__ == '__main__':
    main()
