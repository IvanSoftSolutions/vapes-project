import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

def extract():
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
    options.add_argument(f'user-agent={user_agent}')
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1080, 800) # set the size of the window

    link_list = []
    image_list = []
    name_list = []
    price_list = []
    flavor_list = []

    driver.get("https://tuhumo.com/collections/frontpage")
    driver.find_element(By.ID, "year").send_keys("2003" + Keys.ENTER + Keys.ENTER)
    driver.find_element(By.XPATH, '//input[@value="Enviar"]').click()

    for i in range(1, 4):    
        driver.get(f"https://tuhumo.com/collections/frontpage?filter.v.availability=1&page={i}&sort_by=title-ascending")
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        data = soup.find_all("li", {"class": "grid__item"})
        
        for item in data:
            link = item.find("a")
            link_list.append(link['href'])
            img = item.find("img")
            image_list.append(img['src'])
            name_list.append(img['alt'])
            price = item.find("span", {"class": "price-item price-item--regular"})
            price_list.append(price.text)
        
    for link in link_list:
        driver.get("https://tuhumo.com/" + link)
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        label = soup.find_all("legend", {"class": "form__label"})
        if label:
            flavors = soup.find_all("script", {"type": "application/json"})[-1].contents[0]
            flavor_list.append(flavors)
        else:
            flavor_list.append('Not Applicable')

    driver.quit()

    data_dict = {'Name': name_list, 'Image': image_list, 'Price': price_list, 'Flavors': flavor_list, 'URL': link_list }
    df = pd.DataFrame(data=data_dict) 
    # df.to_csv('vapes_data.csv', index=False)
    return df

def transform(df):
    # df = pd.read_csv('vapes_data.csv')
    
    plumas_filter = df['Name'].str.contains("Plumas")
    df = df[~plumas_filter]
    packman_filter = df['Name'].str.contains("PackMan")
    df = df[~packman_filter]
    backpack_filter = df['Name'].str.contains("BackPack")
    df = df[~backpack_filter]
    paquete_filter = df['Name'].str.contains("Paquete")
    df = df[~paquete_filter]
    
    df['Name'] = df['Name'].str.replace(r'mayoreo', '', regex=True, case=False)
    df['Name'] = df['Name'].str.replace('Exclusivo de TuHumo', '')
    df['Name'] = df['Name'].str.replace(r'\(([^)]+)\)', '', regex=True)
    df['Name'] = df['Name'].str.strip()
    
    df['Price'] = df['Price'].str.replace('$', '')
    df['Price'] = df['Price'].str.replace(',', '')
    df['Price'] = df['Price'].str.strip()
    df['Price'] = df['Price'].astype(float)
    
    flavors = []
    for item in df['Flavors']:
        flavors_info_list= []
        flavor_info = json.loads(item)
        for flavor in flavor_info:
            flavor_info_dict = {flavor['title']: flavor['available']}
            flavors_info_list.append(flavor_info_dict)
        flavors.append(flavors_info_list)

    df['Flavors'] = json.dumps(flavors)
        
    # df.to_csv('vapes_data_clean.csv', index=False)
    return df

def load(df):
    # df = pd.read_csv('vapes_data_clean.csv')
    
    # Database connection details
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    ENDPOINT = ''  # replace with your endpoint
    USER = ''  # replace with your username
    PASSWORD = ''  # replace with your password
    PORT = 5432  # replace with your port
    DATABASE = 'vapes-db'  # replace with your database name

    # Create the connection string
    connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

    # Create the database engine
    engine = create_engine(connection_string)

    # Write the DataFrame to a PostgreSQL table
    df.to_sql('Vapes', engine, if_exists='replace', index=False)

vapes_df = extract()
clean_vapes = transform(vapes_df)
load(clean_vapes)
# print(clean_vapes)