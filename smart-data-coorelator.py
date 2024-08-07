import pandas as pd
import os
import matplotlib.pyplot as plt
from geopy.distance import great_circle
import pickle
import requests
import streamlit as st
from datetime import datetime, timedelta


def read_opr_dump(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    lat_long_line = lines[0]
    lat = float(lat_long_line.split(',')[1].split()[1])
    long = float(lat_long_line.split(',')[2].split()[1])

    column_names_line = lines[21].strip()
    column_names = column_names_line.split()

    data_start_index = 22
    data = []
    for line in lines[data_start_index:]:
        if line.strip():
            row = line.split()
            if len(row) == len(column_names):
                data.append(row)

    if not data:
        return None

    df = pd.DataFrame(data, columns=column_names)

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')

    df['Lat'] = lat
    df['Long'] = long

    return df, lat, long

# Function to clean coordinate strings
def clean_coordinate(coord):
    try:
        return (float(coord[0]), float(coord[1]))
    except ValueError as e:
        print(f"Error cleaning coordinate {coord}: {e}")
        return None

# Function to download file
def download_file(url, folder):
    local_filename = os.path.join(folder, url.split('/')[-1])
    st.write(f"Downloading {url}")
    response = requests.get(url)
    with open(local_filename, 'wb') as f:
        f.write(response.content)
    return local_filename

# Function to extract ZIP file
def extract_zip(zip_path, extract_folder):
    import zipfile
    st.write(f"Extracting {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

# Function to process OPR dump file
def process_file(opr_dump_path, coord):
    lat, long = coord
    processed_file_path = f"./data_{lat}_{long}.csv"
    if not os.path.exists(processed_file_path):
        df, lat, long = read_opr_dump(opr_dump_path)
        if df is not None:
            df.to_csv(processed_file_path, index=False)
    else:
        print(f"Data for {lat}, {long} already processed. Skipping.")
    return processed_file_path



def download_and_process_data_for_coordinate(coord, base_url):
    os.makedirs('./downloads', exist_ok=True)
    os.makedirs('./extracted', exist_ok=True)
    coord_to_grid_point = load_dict_from_pickle('coords_dict.pkl')
    print(f"Loaded coordinates dictionary.")

    if coord not in coord_to_grid_point:
        print(f"Coordinate {coord} not found in the dictionary.")
        return None

    grid_point = coord_to_grid_point[coord]
    zip_url = f"{base_url}{grid_point}.zip"
    zip_filename = f"{grid_point}.zip"
    zip_path = os.path.join('./downloads', zip_filename)

    # Download the file if it doesn't exist
    if not os.path.exists(zip_path):
        
        zip_path = download_file(zip_url, './downloads')

    # Extract the ZIP file
    extract_zip(zip_path, './extracted')

    expected_opr_dump_filename = f"{grid_point}.opr_dump"
    opr_dump_path = os.path.join('./extracted', expected_opr_dump_filename)

    if os.path.exists(opr_dump_path):
        csv_file = process_file(opr_dump_path, coord)
    else:
        print(f"File {opr_dump_path} does not exist.")
        csv_file = None

    # Remove the ZIP file only after processing
    os.remove(zip_path)

    # Clean up extracted files
    for extracted_file in os.listdir('./extracted'):
        os.remove(os.path.join('./extracted', extracted_file))

    print(f"Data for coordinate {coord} processed successfully.")
    return csv_file

def load_dict_from_pickle(pickle_file_path):
    print(f"Loading pickle file from {pickle_file_path}...")
    with open(pickle_file_path, 'rb') as file:
        data_dict = pickle.load(file)
    print(f"Loaded dictionary with {len(data_dict)} items.")
    return data_dict

# Function to find the closest coordinate
def find_closest_coordinate(smartatlantic_coord, coords_dict):
    print(f"SmartAtlantic Coordinate: {smartatlantic_coord}")

    closest_coord = None
    min_distance = float('inf')

    for coord in coords_dict.keys():
        cleaned_coord = clean_coordinate(coord)
        if cleaned_coord is None:
            print(f"Skipping invalid coordinate {coord}.")
            continue

        try:
            distance = great_circle(smartatlantic_coord, cleaned_coord).kilometers
        except Exception as e:
            print(f"Error calculating distance to {cleaned_coord}: {e}")
            continue

        if distance < min_distance:
            min_distance = distance
            closest_coord = cleaned_coord

    if closest_coord is None:
        print("No closest coordinate found.")
    else:
        print(f"Closest Coordinate Found: {closest_coord}")

    return closest_coord

# Function to create a datetime column
def create_datetime_column(df):
    try:
        df['DateTime'] = pd.to_datetime(df['CCYYMM'].astype(str) + df['DDHHmm'].astype(str), format='%Y%m%d%H%M', errors='coerce')
        df.dropna(subset=['DateTime'], inplace=True)
    except Exception as e:
        print(f"Error processing DataFrame: {e}")
    return df

# Function to filter significant wave height
def filter_significant_wave_height(df):
    df = df[df['HS'] > 0]
    return df

# Function to filter data by year range
def filter_by_year_range(df, start_year, end_year):
    if not pd.api.types.is_datetime64_any_dtype(df['DateTime']):
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
    df_filtered = df[(df['DateTime'].dt.year >= start_year) & (df['DateTime'].dt.year <= end_year)]
    return df_filtered

# Function to filter MSC50 data for a coordinate
def filter_data_for_coordinate(csv_file):
    df = pd.read_csv(csv_file)
    df = create_datetime_column(df)
    df = filter_significant_wave_height(df)

    # Get the date range from SmartAtlantic data
    min_date = df_smartatlantic['time'].min()
    max_date = df_smartatlantic['time'].max()

    # Extract year range
    start_year = min_date.year
    end_year = max_date.year

    df_filtered = filter_by_year_range(df, start_year, end_year)
    return df_filtered

# Function to fetch SmartAtlantic data
def fetch_data_from_url(url, local_filename):
    if not os.path.exists(local_filename):
        print("Downloading data from URL...")
        df = pd.read_csv(url, skiprows=1)
        df.to_csv(local_filename, index=False)
    else:
        print("Using cached local data.")
        df = pd.read_csv(local_filename)
    return df

def clean_datetime_strings(df, datetime_column):
    """
    Clean datetime strings in a DataFrame column by removing unwanted characters.

    Parameters:
    - df: DataFrame containing the datetime column.
    - datetime_column: Name of the column with datetime strings.

    Returns:
    - DataFrame with cleaned datetime strings in the specified column.
    """
    if df[datetime_column].dtype == 'object':  # Only clean if column is of type object (string)
        df[datetime_column] = df[datetime_column].str.replace('UTC', '', regex=False).str.strip()
    return df

def convert_to_datetime(df, datetime_column):
    """
    Convert a cleaned datetime column to pandas datetime format and localize to naive.

    Parameters:
    - df: DataFrame containing the datetime column.
    - datetime_column: Name of the column with datetime strings.

    Returns:
    - DataFrame with the datetime column converted to timezone-naive datetime.
    """
    df = clean_datetime_strings(df, datetime_column)
    df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')
    return df


def convert_to_timezone_naive(df, datetime_column):
    df = clean_datetime_strings(df, datetime_column)
    df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce', utc=True)
    df[datetime_column] = df[datetime_column].dt.tz_localize(None)
    return df

# Function to filter data by year range
def filter_by_year_range(df, start_year, end_year):
    if not pd.api.types.is_datetime64_any_dtype(df['DateTime']):
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')
    df_filtered = df[(df['DateTime'].dt.year >= start_year) & (df['DateTime'].dt.year <= end_year)]
    return df_filtered

def clean_smart_atlantic_data(df_smartatlantic):
    df_smartatlantic = convert_to_timezone_naive(df_smartatlantic, 'time')
    df_smartatlantic['time'] = pd.to_datetime(df_smartatlantic['time'], errors='coerce')
    df_smartatlantic['latitude'] = pd.to_numeric(df_smartatlantic['latitude'], errors='coerce')
    df_smartatlantic['longitude'] = pd.to_numeric(df_smartatlantic['longitude'], errors='coerce')
    df_smartatlantic = convert_to_datetime(df_smartatlantic, 'time')
    df_smartatlantic['wave_ht_sig'] = pd.to_numeric(df_smartatlantic['wave_ht_sig'], errors='coerce')
    df_smartatlantic['wind_spd_avg'] = pd.to_numeric(df_smartatlantic['wind_spd_avg'], errors='coerce')
    df_smartatlantic = df_smartatlantic.dropna(subset=['wave_ht_sig'])
    return df_smartatlantic

# Define a dictionary to map friendly names to URLs
dataset_urls = {
    'Red Island Shoal': 'https://www.smartatlantic.ca/erddap/tabledap/SMA_red_island_shoal.csv?station_name%2Ctime%2Clongitude%2Clatitude%2Cprecise_lon%2Cprecise_lat%2Cwind_spd_avg%2Cwind_spd_max%2Cwind_dir_avg%2Cwind_spd2_avg%2Cwind_spd2_max%2Cwind_dir2_avg%2Cair_temp_avg%2Cair_pressure_avg%2Cair_humidity_avg%2Cair_dewpoint_avg%2Csurface_temp_avg%2Cwave_ht_max%2Cwave_ht_sig%2Cwave_period_max%2Cwave_dir_avg%2Cwave_spread_avg%2Ccurr_dir_avg%2Ccurr_spd_avg&time%3E=2010-07-06T19%3A55%3A00Z&time%3C=2024-08-05T00%3A57%3A00Z',
    'St Johns': 'https://www.smartatlantic.ca/erddap/tabledap/SMA_st_johns.csv?station_name%2Ctime%2Clongitude%2Clatitude%2Cwind_spd_avg%2Cwind_spd_max%2Cwind_dir_avg%2Cair_temp_avg%2Cair_pressure_avg%2Csurface_temp_avg%2Cwave_ht_max%2Cwave_ht_sig%2Cwave_period_max&time%3E=2013-07-10T17%3A53%3A01Z&time%3C=2024-08-07T14%3A00%3A01Z',
    # Add more datasets here
}

# Initialize Streamlit app
st.title('Wave and Wind Data Correlator')

# Display buttons for each dataset
selected_dataset = st.selectbox("Select SmartAtlantic Dataset", list(dataset_urls.keys()))

# Get the URL corresponding to the selected dataset
smartatlantic_url = dataset_urls[selected_dataset]
st.write(f"Data from : {smartatlantic_url}")
smartatlantic_file_path = f'{selected_dataset.replace(" ", "_")}_cached.csv'

def fetch_data_from_url(url, local_filename):
    if not os.path.exists(local_filename):
        print("Downloading data from URL...")
        df = pd.read_csv(url, skiprows=1)
        df.to_csv(local_filename, index=False)
    else:
        print("Using cached local data.")
        df = pd.read_csv(local_filename)
    return df

# Fetch SmartAtlantic data based on the selected dataset
df_smartatlantic = fetch_data_from_url(smartatlantic_url, smartatlantic_file_path)
if(selected_dataset == 'Red Island Shoal'):
    st.write("Red Island Shoal chosen!")
    df_smartatlantic.columns = [
        'Unnamed: 0',  # This column can be dropped if not needed
        'time',
        'longitude',
        'latitude',
        'precise_lon',
        'precise_lat',
        'wind_spd_avg',
        'wind_spd_max',
        'wind_dir_avg',
        'wind_spd2_avg',
        'wind_spd2_max',
        'wind_dir2_avg',
        'air_temp_avg',
        'air_pressure_avg',
        'air_humidity_avg',
        'air_dewpoint_avg',
        'surface_temp_avg',
        'wave_ht_max',
        'wave_ht_sig',
        'wave_period_max',
        'wave_dir_avg',
        'wave_spread_avg',
        'curr_dir_avg',
        'curr_spd_avg'
    ]

elif (selected_dataset == 'St Johns'):
    st.write("St Johns data chosen!")

    df_smartatlantic.columns = [
        'station_name',
        'time',
        'longitude',
        'latitude',
        'wind_spd_avg',
        'wind_spd_max',
        'wind_dir_avg',
        'air_temp_avg',
        'air_pressure_avg',
        'surface_temp_avg',
        'wave_ht_max',
        'wave_ht_sig',
        'wave_period_max'
        ]
df_smartatlantic = clean_smart_atlantic_data(df_smartatlantic)

# Setup initial state if not set
if 'date_range' not in st.session_state:
    min_date = df_smartatlantic['time'].min().date()
    max_date = df_smartatlantic['time'].max().date()
    st.session_state.date_range = (min_date, max_date)

valid_coords = df_smartatlantic.dropna(subset=['latitude', 'longitude'])
if not valid_coords.empty:
    smartatlantic_coord = (valid_coords['latitude'].iloc[0], valid_coords['longitude'].iloc[0])
    st.write(f"The coordinates of this SmartAtlantic dataset are ({float(smartatlantic_coord[0]):.6f}, {float(smartatlantic_coord[1]):.6f})")
else:
    st.write("No valid coordinates found in SmartAtlantic data.")
    smartatlantic_coord = None

years = range(df_smartatlantic['time'].dt.year.min(), df_smartatlantic['time'].dt.year.max() + 1)

default_start_year = df_smartatlantic['time'].dt.year.min()
default_end_year = df_smartatlantic['time'].dt.year.max()

# Year range selection dropdowns
start_year = st.selectbox("Select Start Year", years, index=years.index(default_start_year))
end_year = st.selectbox("Select End Year",  years, index=years.index(default_end_year))

if st.button("Generate Plot"):
    if smartatlantic_coord:
        coords_dict = load_dict_from_pickle('coords_dict.pkl')
        closest_coord = find_closest_coordinate(smartatlantic_coord, coords_dict)
        st.write(f"The closest coordinates in MSC 50 dataset were found to be {closest_coord}")
        if closest_coord:
            csv_file = download_and_process_data_for_coordinate(closest_coord, base_url="https://cnodc-cndoc.azure.cloud-nuage.dfo-mpo.gc.ca/public/data-donnees/msc50/atlantic-atlantique/")
            st.write(f"Getting the MSC 50 data corresponding to {closest_coord}")
            if csv_file:
                # Filter MSC50 data based on selected year range
                df_filtered = filter_data_for_coordinate(csv_file)
                filtered_df = filter_by_year_range(df_filtered, start_year, end_year)

                start_datetime = datetime(year=start_year, month=1, day=1)
                end_datetime = datetime(year=end_year, month=1, day=1)

                # Ensure datetime filtering
                df_smartatlantic_filtered = df_smartatlantic[(df_smartatlantic['time'] >= start_datetime) & (df_smartatlantic['time'] <= end_datetime)]
                filtered_df_filtered = filtered_df[(filtered_df['DateTime'] >= start_datetime) & (filtered_df['DateTime'] <= end_datetime)]

                # Create an overlay plot for both datasets
                plt.figure(figsize=(12, 6))

                # Plot SmartAtlantic Significant Wave Height
                plt.plot(df_smartatlantic_filtered['time'], df_smartatlantic_filtered['wave_ht_sig'], label='SmartAtlantic Significant Wave Height', color='orange')

                # Plot MSC50 Significant Wave Height
                plt.plot(filtered_df_filtered['DateTime'], filtered_df_filtered['HS'], label='MSC50 Significant Wave Height', color='blue')

                plt.xlabel('Date')
                plt.ylabel('Significant Wave Height (m)')
                plt.title('Significant Wave Height Over Time (Overlay)')
                plt.legend()
                plt.grid(True)
                st.pyplot(plt, clear_figure=True)

                plt.figure(figsize=(12, 6))

                # Plot SmartAtlantic Wind Speed
                plt.plot(df_smartatlantic_filtered['time'], df_smartatlantic_filtered['wind_spd_avg'], label='SmartAtlantic Avg Wind Speed', color='orange')

                # Plot MSC50 Wind Speed
                plt.plot(filtered_df_filtered['DateTime'], filtered_df_filtered['WS'], label='MSC50 Avg Wind Speed', color='blue')

                plt.xlabel('Date')
                plt.ylabel('Wind Speed (m/s)')
                plt.title('Wind Speed Over Time (Overlay)')
                plt.legend()
                plt.grid(True)
                st.pyplot(plt, clear_figure=True)
