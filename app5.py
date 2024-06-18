import streamlit as st
import snowflake.connector
import pandas as pd

# Function to connect to Snowflake
def connect_to_snow(user, password):
    try:
        ctx = snowflake.connector.connect(
            user=user,
            password=password,
            account='cw37897.east-us-2.azure',
            warehouse='INGEST_WH_ADF',
            database='TEST_DB',
            role='AUTOMATIONINGEST'
        )
        cs = ctx.cursor()
        st.session_state['snow_conn'] = cs
        st.session_state['is_ready'] = True
        st.write("Connected to Snowflake!")
        return cs
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Function to execute query and fetch data
def get_data(query):
    try:
        cs = st.session_state['snow_conn']
        cs.execute(query)
        results = cs.fetch_pandas_all()
        return results
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# Streamlit UI code
def main():
    st.title("Snowflake Query Execution")

    # Sidebar with connection parameters
   # st.sidebar.header("Snowflake Connection Details")
   # account = st.sidebar.text_input("Account URL", "")
    user = st.sidebar.text_input("Username", "")
    password = st.sidebar.text_input("Password", "", type="password")
   # warehouse = st.sidebar.text_input("Warehouse", "")
    #database = st.sidebar.text_input("Database", "")
   # role = st.sidebar.text_input("Role", "")

    # Connect button
    if st.sidebar.button("Log In"):
        if user and password:
            cs = connect_to_snow(user, password)
            if cs:
                st.success("Connection successful!")
        else:
            st.warning("Please enter username and password")

    # Query execution section
    st.header("Execute Query")
    query = st.text_area("Enter your SQL query or type SELECT * FROM TEST_DB.TEST_SCHEMA.PRODUCTS_DEMO", "")
    if st.button("Run Query"):
        if 'snow_conn' in st.session_state:
            results = get_data(query)
            if results is not None:
                st.dataframe(results)
        else:
            st.warning("Please log in first.")

if __name__ == "__main__":
    main()
