from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
import time
import boto3
import re
from decimal import Decimal

def convert_to_decimal(value):
    if isinstance(value, float):
        return Decimal(str(value))
    return value

def initialize_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--incognito')
    options.add_argument('--headless')
    return webdriver.Chrome(options=options)

def close_modal_if_present(driver):
    try:
        modal = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, "getsitecontrol-widget[id*='getsitecontrol-']")))
        close_button = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CLASS_NAME, "button.close")))
        driver.execute_script("arguments[0].click();", close_button)
        WebDriverWait(driver, 1).until(EC.invisibility_of_element(modal))
    except (TimeoutException, NoSuchElementException):
        pass

def process_flyer(driver, wait):
    store_items = defaultdict(dict)
    all_items = set()
    all_stores = set()
    date = ""
    
    try:
        flyers = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "flyer_listing")))

        for i in range(len(flyers)):
            try:
                flyers = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "flyer_listing")))
                flyer = flyers[i]

                close_modal_if_present(driver)

                driver.execute_script("arguments[0].scrollIntoView(true);", flyer)
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable(flyer)).click()
                time.sleep(2)
                close_modal_if_present(driver)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                date = extract_date(soup)

                li_items = soup.find_all('li', class_='flyer_product_info')
                store = soup.find_all('h1', class_='flyer_dealer')[0].get_text(strip=True).split("Flyer")[0].strip()
                all_stores.add(store)
                process_items(li_items, store, store_items, all_items)

                driver.back()
                wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "flyer_listing")))
            except StaleElementReferenceException:
                continue
    except Exception as e:
        print(f"An error occurred: {e}")

    return store_items, all_items, all_stores, date

def extract_date(soup):
    time_tag = soup.find('time')
    date_str = time_tag.get_text(strip=True)
    input_format = "%a %b %d"
    current_year = datetime.now().year
    date_obj = datetime.strptime(f"{current_year} {date_str}", f"%Y {input_format}")
    output_format = "%Y-%m-%d"
    return date_obj.strftime(output_format)

def process_items(li_items, store, store_items, all_items):
    product_name_pattern = r'View product page(.+?)(Model #:|$)'
    sale_price_pattern = r'\$(\d+\.\d+(?:/lb)?)\s*Sale Price'

    for item in li_items:
        product_name_match = re.search(product_name_pattern, item.get_text(strip=True), re.DOTALL)
        sale_price_match = re.search(sale_price_pattern, item.get_text(strip=True))
        product_name = product_name_match.group(1).strip() if product_name_match else None
        sale_price = sale_price_match.group(1) if sale_price_match else None
        if sale_price and product_name:
            store_items[store][product_name] = sale_price
            all_items.add(product_name)

def process_best_prices(store_items, all_items, date):
    best_price = {}
    for item in all_items:
        for store in store_items.keys():
            if item in store_items[store]:
                price = store_items[store][item].split('/')
                if len(price) == 2:
                    if item in best_price:
                        prev_price = best_price[item][0].split('/')
                        best_price[item] = (str(max(float(prev_price[0]), float(price[0]))) + "/lb", store, date)
                    else:
                        best_price[item] = (store_items[store][item], store, date)
                else:
                    if item in best_price:
                        best_price[item] = (max(float(best_price[item][0]), float(store_items[store][item])), store, date)
                    else:
                        best_price[item] = (float(store_items[store][item]), store, date)
    return best_price

def save_to_dynamodb(best_price, table):
    for product, (price, store, flyer_date) in best_price.items():
        partition_key = f"{store}#{product}#{flyer_date}"
        table.put_item(
            Item={
                'StoreName#ProductName#FlyerDate': partition_key,
                'ProductName': product,
                'Price': convert_to_decimal(price),
                'StoreName': store,
                'FlyerDate': flyer_date,
            }
        )

def main():
    driver = initialize_driver()
    url = "https://www.redflagdeals.com/in/ottawa/flyers/categories/groceries/"
    driver.get(url)
    wait = WebDriverWait(driver, 10)

    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('Grocery_Parser')

    store_items, all_items, all_stores, date = process_flyer(driver, wait)
    best_price = process_best_prices(store_items, all_items, date)
    save_to_dynamodb(best_price, table)

    driver.quit()

if __name__ == "__main__":
    main()
