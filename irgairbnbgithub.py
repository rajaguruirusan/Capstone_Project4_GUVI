#importing the necessary libraries
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import base64
import time
import logging
import plotly.express as px

def process_data():
    # Log start time
    start_time = time.time()
    logging.info("Script execution started.")

    # Fetch data
    data = []
    for i in col.find( {}, {'_id': 1, 'name': 1, 'host_id': 1, 'host_name': 1, 'neighbourhood_group': 1, 'neighbourhood': 1,
                            'latitude': 1, 'longitude': 1, 'room_type': 1, 'price': 1, 'minimum_nights': 1, 'number_of_reviews': 1,
                            'last_review': 1, 'reviews_per_month': 1, 'calculated_host_listings_count': 1, 'availability_365': 1,
                            'property_type': 1, 'room_type': 1, 'bed_type': 1,
                            'minimum_nights': 1, 'maximum_nights': 1, 'cancellation_policy': 1, 'accommodates': 1,
                            'bedrooms': 1, 'beds': 1, 'number_of_reviews': 1, 'bathrooms': 1, 'price': 1,
                            'cleaning_fee': 1, 'extra_people': 1, 'guests_included': 1, 'images.picture_url': 1,
                            'review_scores.review_scores_rating': 1} ):
        data.append(i)

    df = pd.DataFrame(data)
    df['images'] = df['images'].apply(lambda x: x['picture_url'])
    df['review_scores'] = df['review_scores'].apply(lambda x: x.get('review_scores_rating', 0))
    df['bedrooms'].fillna(0, inplace=True)
    df['beds'].fillna(0, inplace=True)
    df['bathrooms'].fillna(0, inplace=True)
    df['cleaning_fee'].fillna('Not Specified', inplace=True)
    df['minimum_nights'] = df['minimum_nights'].astype(int)
    df['maximum_nights'] = df['maximum_nights'].astype(int)
    df['bedrooms'] = df['bedrooms'].astype(int)
    df['beds'] = df['beds'].astype(int)
    df['bathrooms'] = df['bathrooms'].astype(str).astype(float)
    df['price'] = df['price'].astype(str).astype(float).astype(int)
    df['cleaning_fee'] = df['cleaning_fee'].apply(lambda x: int(float(str(x))) if x != 'Not Specified' else 'Not Specified')
    df['extra_people'] = df['extra_people'].astype(str).astype(float).astype(int)
    df['guests_included'] = df['guests_included'].astype(str).astype(int)

    host = []
    for i in col.find( {}, {'_id':1, 'host':1}):
        host.append(i)

    df_host = pd.DataFrame(host)
    host_keys = list(df_host.iloc[0,1].keys())
    host_keys.remove('host_about')

    for i in host_keys:
        if i == 'host_response_time':
            df_host['host_response_time'] = df_host['host'].apply(lambda x: x['host_response_time'] if 'host_response_time' in x else 'Not Specified')
        else:
            df_host[i] = df_host['host'].apply(lambda x: x[i] if i in x and x[i]!='' else 'Not Specified')

    df_host.drop(columns=['host'], inplace=True)
    df_host['host_is_superhost'] = df_host['host_is_superhost'].map({False:'No',True:'Yes'})
    df_host['host_has_profile_pic'] = df_host['host_has_profile_pic'].map({False:'No',True:'Yes'})
    df_host['host_identity_verified'] = df_host['host_identity_verified'].map({False:'No',True:'Yes'})

    df_host['host_is_superhost'] = df_host['host_is_superhost'].map({False:'No',True:'Yes'})
    df_host['host_has_profile_pic'] = df_host['host_has_profile_pic'].map({False:'No',True:'Yes'})
    df_host['host_identity_verified'] = df_host['host_identity_verified'].map({False:'No',True:'Yes'})

    address = []
    for i in col.find( {}, {'_id':1, 'address':1}):
        address.append(i)

    df_address = pd.DataFrame(address)
    address_keys = list(df_address.iloc[0,1].keys())

    for i in address_keys:
        if i == 'location':
            df_address['location_type'] = df_address['address'].apply(lambda x: x['location']['type'])
            df_address['longitude'] = df_address['address'].apply(lambda x: x['location']['coordinates'][0])
            df_address['latitude'] = df_address['address'].apply(lambda x: x['location']['coordinates'][1])
            df_address['is_location_exact'] = df_address['address'].apply(lambda x: x['location']['is_location_exact'])
        else:
            df_address[i] = df_address['address'].apply(lambda x: x[i] if x[i]!='' else 'Not Specified')

    df_address.drop(columns=['address'], inplace=True)
    df_address['is_location_exact'] = df_address['is_location_exact'].map({False:'No',True:'Yes'})

    availability = []
    for i in col.find( {}, {'_id':1, 'availability':1}):
        availability.append(i)

    df_availability = pd.DataFrame(availability)
    availability_keys = list(df_availability.iloc[0,1].keys())

    for i in availability_keys:
        df_availability['availability_30'] = df_availability['availability'].apply(lambda x: x['availability_30'])
        df_availability['availability_60'] = df_availability['availability'].apply(lambda x: x['availability_60'])
        df_availability['availability_90'] = df_availability['availability'].apply(lambda x: x['availability_90'])
        df_availability['availability_365'] = df_availability['availability'].apply(lambda x: x['availability_365'])

    df_availability.drop(columns=['availability'], inplace=True)

    def amenities_sort(x):
        a = x
        a.sort(reverse=False)
        return a

    amenities = []
    for i in col.find( {}, {'_id':1, 'amenities':1}):
        amenities.append(i)

    df_amenities = pd.DataFrame(amenities)
    df_amenities['amenities'] = df_amenities['amenities'].apply(lambda x: amenities_sort(x))

    df = pd.merge(df, df_host, on='_id')
    df = pd.merge(df, df_address, on='_id')
    df = pd.merge(df, df_availability, on='_id')
    df = pd.merge(df, df_amenities, on='_id')
    # Log end time and calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time

    # Convert execution time to mm:ss format
    minutes, seconds = divmod(execution_time, 60)
    time_format = "{:02}:{:02}".format(int(minutes), int(seconds))

    logging.info(f"Data Processed in {execution_time:.2f} seconds")
    # Display execution time in Streamlit
    st.write(f"Data Processed in {time_format} minutes:seconds")

    return df

def download_csv(df):
    # Convert DataFrame to CSV and generate download link
    csv = df.to_csv(index=False).encode('utf-8')
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="airbnb_processed.csv">Download CSV File</a>'
    return href

# Main Streamlit app
if __name__ == "__main__":
    # Main Streamlit code starts
    # SETTING PAGE CONFIGURATIONS
    icon = Image.open("guvi_logo.png")
    st.set_page_config(
        page_title="Airbnb Analysis | By IRG",
        page_icon= icon,
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={'About': """# This application is created for GUVI Capstone Project4 by *IRG!*"""}
    )
    st.header("Airbnb Analysis | By IRG")
    
    # CREATING OPTION MENU
    with st.sidebar:
        selected = option_menu(None, ["Home", "Explore Data", "Exploratory Data Analysis"], 
                                icons=["house-door-fill","plus-circle","search"],
                                default_index=0,
                                orientation="vertical",
                                styles={"nav-link": {"font-size": "24px", "text-align": "centre", "margin": "0px", 
                                                        "--hover-color": "#FF4B4B"},
                                        "icon": {"font-size": "24px"},
                                        "container" : {"max-width": "7000px"},
                                        "nav-link-selected": {"background-color": "Reds"}})
    # HOME MENU
    if selected == "Home":
        col1,col2 = st.columns(2)
        with col1:
            st.markdown("#### :red[**Technologies Used :**] Python scripting, Data Preprocessing, Visualization, EDA, Streamlit, MongoDb, Tableau")
            st.markdown("#### :red[**EDA Domain :**] Travel Industry, Property Management and Tourism")
        with col2:
            st.image("guvi_logo.png")

    # EXPLORE DATA MENU
    if selected == "Explore Data":
        st.markdown("### MongoDB Connection")
        st.markdown("#    ")
        st.write("### Paste MongoDB Connection Code:")
        uri = st.text_input("Sample Code : mongodb+srv://username:<password>@cluster0.ifo6y0s.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0").split(',')
        if uri and st.button("Connect MongoDB"):
            # Create a new client and connect to the server
            client = MongoClient(uri, server_api=ServerApi('1'))
            # Send a ping to confirm a successful connection
            try:
                client.admin.command('ping')
                st.success("Successfully connected to MongoDB!")

                # Check for 'sample_airbnb' database availability
                if 'sample_airbnb' in client.list_database_names():
                    st.markdown("#### `sample_airbnb` database is available.")
                    db = client['sample_airbnb']
                    collections = db.list_collection_names()
                    for collection in collections:
                        count = db[collection].count_documents({})
                        st.write(f"Collection name: `{collection}`, Record Count: `{count}`")
                        col= db["listingsAndReviews"] 
                        st.spinner("Processing Data")
                        df = process_data()
                        st.write(df.head())
                        st.markdown("`Data processed successfully!`")
                        
                        st.markdown(download_csv(df), unsafe_allow_html=True)
                else:
                    st.markdown("`sample_airbnb` database not available in the MongoDB")

            except Exception as e:
                st.error(f"Failed to connect to MongoDB: {e}")
    
    room_type_df = None
    # Exploratory Data Analysis
    if selected == "Exploratory Data Analysis":
        fl = st.file_uploader(":file_folder: Upload a file", type=["csv", "txt", "xlsx", "xls"])
        if fl is not None:
            filename = fl.name
            st.write(filename)
            
            # Check the file extension to use the appropriate pandas function
            if filename.endswith('.csv') or filename.endswith('.txt'):
                df = pd.read_csv(fl, encoding="ISO-8859-1")
            elif filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(fl)
            st.write(df.head())

            col1, col2 = st.columns(2)

            # Select neighbourhood_group
            with col1:
                neighbourhood_group = st.multiselect("Select Neighbourhood Groups", df["market"].unique())
            
            # Select neighbourhood
            with col2:
                # This needs to be dynamically updated based on neighbourhood_group selection
                if neighbourhood_group:
                    neighbourhood = st.multiselect("Select Neighbourhoods", df[df["market"].isin(neighbourhood_group)]["host_neighbourhood"].unique())
                else:
                    neighbourhood = st.multiselect("Select Neighbourhoods", df["host_neighbourhood"].unique())

            # Filter the data based on neighbourhood_group, neighbourhood
            if neighbourhood_group:
                df = df[df["market"].isin(neighbourhood_group)]
            if neighbourhood:
                df = df[df["host_neighbourhood"].isin(neighbourhood)]

            # Perform your analysis on df after filtering
            room_type_df = df.groupby(by=["room_type"], as_index=False)["price"].mean()
        if room_type_df is not None:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Room Type vs Average Price Chart")
                fig = px.bar(room_type_df, x="room_type", y="price", text=['${:,.2f}'.format(x) for x in room_type_df["price"]],
                            template="seaborn")
                st.plotly_chart(fig, use_container_width=True, height=200)
            with col2:
                st.subheader("Neighbourhood Group vs Average Price Chart")
                fig = px.pie(df, values="price", names="market", hole=0.5)
                fig.update_traces(text=df["market"], textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

            col1, col2 = st.columns((2))
            with col1:
                st.subheader("Room Type wise Average Price Data")
                st.write(room_type_df.style.background_gradient(cmap="Reds"))

            with col2:
                st.subheader("Neighbourhood Group wise Average Price Data")
                neighbourhood_group = df.groupby(by="market", as_index=False)["price"].mean()
                st.write(neighbourhood_group.style.background_gradient(cmap="Reds"))

            # Scatter plot
            data1 = px.scatter(
                df,
                x="market",
                y="host_neighbourhood",
                color="room_type",
                size_max=10,
                opacity=0.8,
                color_discrete_map={
                    "Entire home/apt": "blue",
                    "Private room": "green",
                    "Shared room": "red"
                }  
            )
            data1.update_layout(
                title="Scatter Plot for Neighbourhood and Neighbourhood_Group",
                titlefont=dict(size=20),
                xaxis=dict(
                    title="Neighbourhood_Group",
                    titlefont=dict(size=18),
                    gridcolor='LightGrey'
                ),
                yaxis=dict(
                    title="Neighbourhood",
                    titlefont=dict(size=18),
                    gridcolor='LightGrey'
                ),
                legend=dict(
                    title='Room Type',
                    font=dict(size=16),
                    bgcolor='White',
                    bordercolor='Black'
                )
            )
            st.plotly_chart(data1, use_container_width=True)

            st.subheader("Details of Room Availability and Price View Data in the Neighbourhood")
            st.write(df.iloc[:500, 1:20:2].style.background_gradient(cmap="Oranges"))

            # map function for room_type
            st.subheader("Airbnb Analysis in Map view")
            df_map = df.rename(columns={"Latitude": "lat", "Longitude": "lon"})
            st.map(df_map)
        else:
            # If no file has been uploaded, prompt the user to upload a file
            st.write("Please upload a file to continue.")