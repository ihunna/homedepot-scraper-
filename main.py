from homedepot import HomeDepot
import os, csv, concurrent.futures
from datetime import datetime






def main():
    homedepot = HomeDepot()

    products = homedepot.load_products('Reduced product List 2025 10 30.csv')
    headers = ['name', 'brand', 'url', 'mainImageurl', 'SKU', 'Reviews', 'Rating', 'Model', 'retailer', 'storesku', 'omsid','storeName','storeID','storeLocation','inventory']
    csv_file = f'product-{datetime.now().strftime("%Y-%m-%d")}.csv'
    results_folder = os.path.join(homedepot.root_dir, 'results')

    if not os.path.exists(results_folder):
        os.makedirs(results_folder)

    with open(f'{results_folder}/{csv_file}', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)


    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_product = {
            executor.submit(
                homedepot.scan_wholestore,
                product,
                f'{results_folder}/{csv_file}',
                delay=3,
                timeout=60
            ): product for product in products
        }

        for future in concurrent.futures.as_completed(future_to_product):
            product = future_to_product[future]
            try:
                success, result = future.result()
                if success:
                    print(result)
                else:
                    print(f'Failed scanning for product {product["SKU"]}: {result}')
            except Exception as e:
                print(f'Exception scanning product {product["SKU"]}: {e}')
    # Utils.deduplicate_csv(f'{results_folder}/{csv_file}')


if __name__ == '__main__':
    main()