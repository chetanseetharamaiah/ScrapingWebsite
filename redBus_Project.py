import streamlit as st
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time,csv
import re,os,glob
from datetime import datetime
import mysql.connector
from sqlalchemy.exc import SQLAlchemyError

class RedBus_Scrape:
    
    def  __init__(self):
        self.url = 'https://www.redbus.in/'
        self.original_window = ''
        self.busname_l = []
        self.buslink_l = []
        self.Bus_Route_Link = {} #holds all the states and link to open the state link
        self.bus_full_list = []
        self.dbname = "redBus"
        self.file_name = "redBus"
        
    def fetch_the_website_info(self):
        self.driver = webdriver.Chrome()
        print("test start and the time is ",datetime.now())
        self.driver.get(self.url)
        time.sleep(4)
        self.driver.maximize_window()
        st.write("Opening redBus Website")
        self.get_states_info()

    def switch_to_new_window(self):
        """Switches WebDriver to the newly opened window."""
        self.original_window = self.driver.current_window_handle
        WebDriverWait(self.driver, 10).until(EC.new_window_is_opened)
        for window_handle in self.driver.window_handles:
            if window_handle != self.original_window:
                self.driver.switch_to.window(window_handle)
    
    def get_states_info(self):
        try:
            directory = WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div.rtcHeadViewAll a')))
            directory.click()
            self.switch_to_new_window()
            links = WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.D113_ul_rtc a')))
            for link in links:
                href = link.get_attribute('href')
                text = link.text.strip()
                self.Bus_Route_Link[text] = href
        except Exception as e:
            print("error in get_states_info")
            print("Error:", e)
        self.open_state()
        self.copy_to_csv_file(self.bus_full_list)

    def open_state(self):
        try:
            for bus,link in self.Bus_Route_Link.items():
                state_name_str = "_".join(re.findall(r'\b[A-Z]+\b', bus))
                st.write(f"STATE &nbsp;&nbsp;&nbsp;&nbsp;{state_name_str}")
                st.write(f"{link}")
                time.sleep(1)
                self.driver.execute_script(f"window.open('{link}');")
                time.sleep(3)
                self.driver.switch_to.window(self.driver.window_handles[-1])  # Switch to the latest tab
                # fetching the number of buse routes in each page and the number of pages in the particular state and add them into a list for later use
                try:
                    page_numbers = self.driver.find_elements(By.CSS_SELECTOR, "div.DC_117_pageTabs")
                    page_count = len(page_numbers)
                except Exception as e:
                    print(f"Error is {e}")
            
                if page_count >= 1:
                    self.scrape_route_infos(page_numbers,state_name_str,link)
                    time.sleep(2)
                    self.driver.close()
                    self.driver.switch_to.window(self.original_window)
                else:
                    st.write(f"{state_name_str} This link has no data")
            print("test end and the time is ",datetime.now())
        except Exception as e:
            print("error in open_state")
            print("Error:", e)
       
    def scrape_route_infos(self,page_numbers,state_name_str,state_link):
        try:
            for i in range(len(page_numbers)):
                self.driver.execute_script("arguments[0].scrollIntoView();", page_numbers[i])
                time.sleep(1)
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable(page_numbers[i])).click()
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
                self.bus_link()
            self.get_bus_details(state_name_str,state_link)
            self.busname_l.clear()
            self.buslink_l.clear()
        except Exception as e:
            print("error at scrape_route_infos")
            print(f"Error at {e}")
    
    def copy_to_csv_file(self,bus_full_list):
        try:
            csv_file_name = self.create_csv_file(f'{self.file_name}')
            with open(csv_file_name, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for busdetail in bus_full_list:
                    if isinstance (busdetail, list):
                        for busdetails in busdetail:
                            if isinstance(busdetails,dict):
                                raw_data = [
                                        busdetails.get('state',''),
                                        busdetails.get('routes',''),
                                        busdetails.get('routelink',''),
                                        busdetails.get('operator',''),
                                        busdetails.get('busid',''),
                                        busdetails.get('busname',''),
                                        busdetails.get('bustype',''),
                                        busdetails.get('dptime',''),
                                        busdetails.get('dploc',''),
                                        busdetails.get('bdtime',''),
                                        busdetails.get('bdloc',''),
                                        busdetails.get('rating',''),
                                        busdetails.get('fare',''),
                                        busdetails.get('avilableseats',''),
                                        busdetails.get('windowseats','')]
                                writer.writerow(raw_data)
        except Exception as e:
            print("Error why writting the bus full details to csv")
            print(f"Error code is {e}")
    def bus_link(self):
        #fetching every route name and corresponding link for the route of particular state
        try:
            time.sleep(2)
            routes = self.driver.find_elements(By.CSS_SELECTOR, 'div.route_details a.route')
            for route in routes:
                link = route.get_attribute('href')
                name = route.get_attribute('title')
                self.busname_l.append(name)
                self.buslink_l.append(link)
        except Exception as e:
            print("error at bus link")
            print(f"Error occurred: {e}")

    def get_bus_details(self,state_name_str,state_link):
        try:
            for i in range(len(self.buslink_l)):
                st.write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ROUTE {self.busname_l[i]}--->  {self.buslink_l[i]}")
                self.driver.get(self.buslink_l[i])
                time.sleep(2)
                check_for_gvt_bus = self.driver.find_elements(By.CSS_SELECTOR, "div.group-data.clearfix")
                check_for_pvt_bus = self.driver.find_elements(By.CSS_SELECTOR, 'ul.bus-items')
                st.write("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Fetching government and private bus details")
                if check_for_gvt_bus:
                    
                    bus_details_gvt = self.get_gvt_bus_details(check_for_gvt_bus,state_name_str,self.busname_l[i],self.buslink_l[i])
                    self.bus_full_list.append(bus_details_gvt)
                    time.sleep(2)
                if check_for_pvt_bus:
                    bus_details_pvt = self.get_pvt_bus_details_in_route(state_name_str,self.busname_l[i],self.buslink_l[i])
                    self.bus_full_list.append(bus_details_pvt)
                    time.sleep(2)
                else:
                    st.write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{self.busname_l[i]} doesnt have any data")
                st.write("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Successfully scraped  bus details")
            st.write(f"Successfully scraped the {state_name_str} state")
            
        except Exception as e:
            print("error at get_bus_details")
            print(f"error is {e}")
    def create_csv_file(self,state_name):
        try:
            csv_file_name = state_name+".csv"
            column_names = ['state','routes','routelink','operator','busid','busname','bustype','dptime','dploc','bdtime','bdloc','rating','fare','avilableseats','windowseats']
            with open(csv_file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(column_names)
                file.close()
            return csv_file_name
        except Exception as e:
            print("error in create_csv_file")
            print("Error:", e)

    def get_gvt_bus_details(self,r_route_details,state_name,bus_route,route_link):
        try:
            time.sleep(1)
            for r1_route_elements in  r_route_details:                       
                r1_route_elements = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.gmeta-data.clearfix")))
                r2_route_bus_check = r1_route_elements.find_element(By.CLASS_NAME, "button")
                try:
                    r2_route_bus_check.click()
                except Exception as e:
                    print(f"error is {e}")
                time.sleep(1)
                bus_list = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.bus-items')))
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                while True:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)  # Wait for the content to load
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break  # Break if no new content is loaded
                    last_height = new_height
                bus_items = WebDriverWait(bus_list, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.row-sec")))
                time.sleep(1)
                self.driver.execute_script("document.body.scrollIntoView({behavior: 'smooth', block: 'start'});")
                bus_gvt_list = self.fetch_bus_information(bus_items,state_name,bus_route,route_link,operator='Government')
                self.driver.execute_script("document.body.scrollIntoView({behavior: 'smooth', block: 'start'});")
                time.sleep(1)
                r2_route_bus_check.click()
                            
        except Exception as e:
            print("Error are gvt bus details")
            print(f"Error occurred: {e}")
        return bus_gvt_list
    def get_pvt_bus_details_in_route(self,state_name,bus_route,route_link):
        try:
            bus_list = self.driver.find_element(By.CSS_SELECTOR, 'ul.bus-items')
            WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.bus-items")))
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Wait for the content to load
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break  # Break if no new content is loaded
                last_height = new_height
            bus_items = WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.row-sec")))
            time.sleep(1)
            bus_pvt_list = self.fetch_bus_information(bus_items,state_name,bus_route,route_link,operator='Private')
            self.driver.execute_script("document.body.scrollIntoView({behavior: 'smooth', block: 'start'});")
            
        except Exception as e:
            print("error at get_pvt_bus_details_in_route")
            print(f"Error occurred: {e}")
        return bus_pvt_list
    
    def fetch_bus_information(self,bus_items,state_name,bus_route,route_link,operator):
        fetched_bus_info = []
        for bus_info in bus_items:
            try:
                bus_id = bus_info.get_attribute("id")
                bus_name = bus_info.find_element(By.CLASS_NAME, "travels").text
                bus_type = bus_info.find_element(By.CLASS_NAME, "bus-type").text
                departure_time = bus_info.find_element(By.CLASS_NAME, "dp-time").text
                departure_loc = bus_info.find_element(By.CLASS_NAME, "dp-loc").text
                boarding_time = bus_info.find_element(By.CLASS_NAME, "bp-time").text
                try:
                    boarding_loc = bus_info.find_element(By.CLASS_NAME, "bp-loc").text
                except Exception as e:
                    boarding_loc = "NA"
                try:
                    rating = bus_info.find_element(By.CSS_SELECTOR, "div.rating-sec.lh-24 span").text
                except Exception as e:
                    rating = "NA"
                fare = bus_info.find_element(By.CSS_SELECTOR, "span.f-19.f-bold").text
                try:
                    avl_seat = bus_info.find_element(By.CLASS_NAME, "seat-left").text
                    avl_seats = avl_seat.split()[0]
                except Exception as e:
                    avl_seats = "NA"
                try:
                    window_seats = bus_info.find_element(By.CLASS_NAME, 'window-left').text
                    window_seat = window_seats.split()[0]
                except Exception as e:
                    window_seat = "NA"
                
                bus_details = {
                    'state':state_name,
                    'routes':bus_route,
                    'routelink':route_link,
                    'operator':operator,
                    'busid':bus_id,
                    'busname':bus_name,
                    'bustype':bus_type,
                    'dptime':departure_time,
                    'dploc':departure_loc,
                    'bdtime':boarding_time,
                    'bdloc':boarding_loc,
                    'rating':rating,
                    'fare':fare,
                    'avilableseats':avl_seats,
                    'windowseats':window_seat
                }
                fetched_bus_info.append(bus_details)
            except Exception as e:
                print("error at Fetching all bus details")
                print(f"Error occurred: {e}")
        return fetched_bus_info

    def create_db_add_bus_info(self):
        print("db test start and the time is ",datetime.now())
        st.markdown(
            """
            <style> 
            .custom-title {
                text-align: center;
                font-size:20px;
                font-weight:bold;
            }
            </style>
            """, unsafe_allow_html=True)

        # Display the title with the custom font size
        st.markdown('<p class="custom-title">Creating table and copying the scraped values into MySQL</p>', unsafe_allow_html=True)
        with st.spinner('Processing...'):
            con = mysql.connector.connect(
            host="localhost",
            user="root",
            password="123456789"
            )
            cursor = con.cursor()
            time.sleep(3)
            
            query = f"create database if not exists {self.dbname}"  #-> to create   keep this
            cursor.execute(query) # -> keep this
            time.sleep(3)
            
            query = f"use {self.dbname}"
            cursor.execute(query)
            
            query = """create table if not exists businfo( id int AUTO_INCREMENT PRIMARY KEY,
                                            state text,
                                            routes text,
                                            routelink text,
                                            operator text,
                                            busid int,
                                            busname text,
                                            bustype text,
                                            dptime TIME,
                                            dploc text,
                                            bdtime TIME,
                                            bdloc text,
                                            rating float(5, 1),
                                            fare decimal(10, 2),
                                            avilableseats int,
                                            windowseats int
                                            )"""
            try:
                cursor.execute(query);
            except SQLAlchemyError as e:
                    print(f"Error: {e}")
            time.sleep(2)
            #con.close()
            engine = create_engine(f'mysql+pymysql://root:123456789@localhost:3306/{self.dbname}')
            
            current_directory = os.getcwd()
            csv_files = glob.glob(os.path.join(current_directory, f"{self.dbname}.csv"))
            if csv_files:
                for file in csv_files:
                    filename = os.path.basename(file)
                    df = pd.read_csv(filename,encoding = 'iso-8859-1')
                    try:
                        df.to_sql('businfo', con=engine, if_exists='append', index=False)
                        
                        st.write(f"{filename} data inserted successfully")
                        time.sleep(2)
                    except SQLAlchemyError as e:
                        print(f"Error: {e}")
                st.success('successfully created table and copied the data into table!')
            else:
                st.write("Sorry am unable to find the files to ")
        print("db test end and the time is ",datetime.now())
        
    def fetch_the_deatils_from_DB(self):
        st.title('hey! here go your bus deatils')
        connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='123456789',
        database=f'{self.dbname}'
        )
        def change_time_format(df):
            df['bdtime'] = df['bdtime'].apply(lambda x: str(x).split(" ")[-1])
            df['dptime'] = df['dptime'].apply(lambda x: str(x).split(" ")[-1])
            df['busid'] = df['busid'].astype(str)
            return df
        def time_selection(prefix):
            time_options1 = [f"{h:02d}" for h in range(24)]
            time_options2 = [f"{m:02d}" for m in range(60)]
            time_options3 = [f"{s:02d}" for s in range(60)]
            col1, col2, col3 = st.columns(3)  # Use columns to align them in a single row
            with col1:
                selected_hour = st.selectbox("HH", time_options1, key=f"{prefix}_hour")
            with col2:
                selected_minute = st.selectbox("MM", time_options2, key=f"{prefix}_minute")
            with col3:
                selected_second = st.selectbox("SS", time_options3, key=f"{prefix}_second")
            selected_time = f"{selected_hour}:{selected_minute}:{selected_second}"
            return selected_time
        
        query = f"SELECT * FROM {self.dbname}.businfo"
        df = pd.read_sql(query, connection)
        df = change_time_format(df)  
        state = df['state'].drop_duplicates().tolist()
        state.insert(0, 'Select your option')
        sta = st.selectbox('states',state)
        if sta != 'Select your option':
            query = f""" SELECT * 
                                FROM {self.dbname}.businfo 
                                WHERE state = '{sta}'
                    """
            df = pd.read_sql(query, connection)
            route = df['routes'].drop_duplicates().tolist()
            route.insert(0, 'Select your option')
            rt = st.selectbox('routes', route)
            if rt != 'Select your option':
                op = df['operator'].drop_duplicates().tolist()
                op.insert(0,'Select your option')
                opt = st.selectbox('operator',op)
                if opt != 'Select your option':
                    bustype = df['bustype'].drop_duplicates().tolist()
                    bustype.insert(0,'Select your option')
                    bus_type = st.selectbox('bustype',bustype)
                    if bus_type != 'Select your option':
                        rating = st.slider('rating',min_value=1.0,max_value=5.0,step=0.1) 
                        if rating >= 1:
                            fare = st.slider('fare',min_value=500,max_value=2000,step=50)
                            if fare > 100:
                                avilableseat = st.slider('avilableseat',min_value=1,max_value=10,step=1)
                                if avilableseat > 0:
                                    windowseat = st.slider('windowseat',min_value=0,max_value=10,step=1)
                                    if windowseat >= 0:
                                        st.write("Select the time in 24hour format")
                                        st.write(f"departure time")
                                        dptime = time_selection(prefix = "dptime")
                                        st.write(f"boarding time")
                                        bdtime = time_selection(prefix = "bdtime")
                                        query = f""" SELECT *
                                        FROM {self.dbname}.businfo
                                        WHERE state = '{sta}'and routes = '{rt}' and operator = '{opt}' and bustype = '{bus_type}' and rating > '{rating}' and fare <= '{fare}' and avilableseats >= '{avilableseat}' and windowseats >= '{windowseat}' and dptime >= '{dptime}' and bdtime <= '{bdtime}'
                                        """
                                        df = pd.read_sql(query, connection)
                                        df = change_time_format(df)
                                        st.dataframe(df)
                                        
                                    else:
                                        query = f""" SELECT *
                                        FROM {self.dbname}.businfo
                                        WHERE state = '{sta}'and routes = '{rt}' and operator = '{opt}' and bustype = '{bus_type}' and rating > '{rating}' and fare <= '{fare}' and avilableseats >= '{avilableseat}'
                                        """
                                        df = pd.read_sql(query, connection)
                                        df = change_time_format(df)
                                        st.dataframe(df)
                                else:
                                    query = f""" SELECT *
                                        FROM {self.dbname}.businfo
                                        WHERE state = '{sta}'and routes = '{rt}' and operator = '{opt}' and bustype = '{bus_type}' and rating > '{rating}' and fare <= '{fare}' 
                                        """
                                    df = pd.read_sql(query, connection)
                                    df = change_time_format(df)
                                    st.dataframe(df)
                            else:
                                query = f""" SELECT *
                                        FROM {self.dbname}.businfo
                                        WHERE state = '{sta}'and routes = '{rt}' and operator = '{opt}' and bustype = '{bus_type}'  and rating >= '{rating}' 
                                    """
                                df = pd.read_sql(query, connection)
                                df = change_time_format(df)
                                st.dataframe(df)
                        else:
                            query = f""" SELECT *
                                        FROM {self.dbname}.businfo
                                        WHERE state = '{sta}'and routes = '{rt}' and operator = '{opt}' and bustype = '{bus_type}'
                                    """
                            df = pd.read_sql(query, connection)
                            df = change_time_format(df)
                            st.dataframe(df)
                    else:
                        query = f""" SELECT * 
                                    FROM {self.dbname}.businfo 
                                    WHERE state = '{sta}' and routes = '{rt}' and operator = '{opt}'  
                                    """
                        df = pd.read_sql(query, connection)
                        df = change_time_format(df)
                        st.dataframe(df)
                else:
                    query = f""" SELECT * 
                                    FROM {self.dbname}.businfo 
                                    WHERE state = '{sta}' and routes = '{rt}'  
                                    """
                    df = pd.read_sql(query, connection)
                    df = change_time_format(df)
                    st.dataframe(df)
            else:
                query = f""" SELECT * 
                                FROM {self.dbname}.businfo 
                                WHERE state = '{sta}' """
                df = pd.read_sql(query, connection)
                df = change_time_format(df)
                st.dataframe(df)
        else:
            st.dataframe(df)
project = RedBus_Scrape()

sidebar_style = """
    <style>
    [data-testid="stSidebar"] {
        border: 2px solid #FF6347;
        padding: 10px;
        border-radius: 10px;
    }
    </style>
"""
# Inject custom CSS into Streamlit app
st.markdown(sidebar_style, unsafe_allow_html=True)
d = st.sidebar.header("Welcome!")

if d:
    option1 = st.sidebar.button('fetch states and bus routes')
    if option1:
        st.session_state.page = 'scrape_redbus_website'
    option2 = st.sidebar.button('Copy route info to Mysql')
    if option2:
        st.session_state.page = 'copy_csv_values_to_mysql'
    option3 = st.sidebar.button('fetch the info from Mysql')
    if option3:
        st.session_state.page = 'fetch_db_from_mysql'

st.markdown("""
    <style>
    .centered-title {
        display: flex;
        justify-content: center;
        align-items: center;
        font-size: 30px;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)
st.markdown('<div class="centered-title">redBus</div>', unsafe_allow_html=True)
st.markdown('<div class="centered-title">Indias No.1 Online Bus Ticket Booking Site</div>', unsafe_allow_html=True)
img = st.image("C:/Users/cheth/GUVI/Project_1/Redbus_image.png")

if 'page' not in st.session_state:
    st.session_state.page = 'home'  # Default to home page
elif st.session_state.page == 'scrape_redbus_website':
    project.fetch_the_website_info()
elif st.session_state.page == 'copy_csv_values_to_mysql':
    project.create_db_add_bus_info()
elif st.session_state.page == 'fetch_db_from_mysql':
    project.fetch_the_deatils_from_DB()
