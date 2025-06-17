import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2

# Function to fetch data from Redshift
def fetch_data():
    host = 'analytics-redshift.ckzwtwo4amnp.ap-south-1.redshift.amazonaws.com'
    port = 5439
    dbname = 'fact_tables'
    user = 'arvind_saravanan'
    password = 'D14PreeTT68KU5'

    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode='require'
        )
        print("Connection to Redshift established successfully.")

        cursor = connection.cursor()
        query = """
        SELECT
            u.contractid,
            u.contracttypename AS "category",
            u.mborderid AS "order id",
            u.orderitemname AS "package name",
            u.createdon,
            u.fulfillmenttype AS "visit type",
            u.fulfillmentdate AS "appointment date",
            u.patientuserrelation AS "relation",
            u.patientgender AS "gender",
            u.patientage,
            u.issponsored,
            u.providerlocality AS "location",
            u.providercity AS "city",
            u.providerpincode AS "pincode",
            u.providerstate AS "state",
            u.pmentityid AS "entity id",
            u.misdate,
            CASE 
                WHEN u.patientage < 30 THEN '< 30'
                WHEN u.patientage BETWEEN 30 AND 39 THEN '30-40'
                WHEN u.patientage BETWEEN 40 AND 49 THEN '40-50'
                ELSE '50+'
            END AS "age bracket"
        FROM
            unifiedtransactions_unifiedtable u
        WHERE
            u.contractid = 9716
            AND u.pmentityid IN (1006205,1071187)
            AND u.requeststatusid IN (32, 33, 37, 48, 134)
            AND u.misdate >= '2025-06-10'
            AND u.issponsored = TRUE
        """
        cursor.execute(query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        df['misdate'] = pd.to_datetime(df['misdate']).dt.date
        cursor.close()
        connection.close()
        print("Connection closed.")
        return df

    except Exception as e:
        print("Error connecting to Redshift:", e)
        return pd.DataFrame()

# Streamlit app
def main():
    st.sidebar.title("Login")
    username = st.sidebar.text_input("Username", key="username")
    password = st.sidebar.text_input("Password", type="password", key="password")

    df = fetch_data()

    # Debugging
    #st.write("Columns in DataFrame:", df.columns)
    #st.write("DataFrame Preview:", df.head())

    if username and password:
        expected_password = f"{username}_medibuddy"
        if password == expected_password:
            st.title(f"HRWIN Dashboard for entity id: {username}")
            st.success(f"Welcome, HR representative for entity {username}")

            if 'entity id' in df.columns:
                df = df[df['entity id'] == int(username)]

                if not df.empty:
                    entity_ids = df['entity id'].unique()
                    selected_entity = st.selectbox("Select entity id", entity_ids)

                    min_date = df['misdate'].min()
                    max_date = df['misdate'].max()
                    selected_date = st.date_input("Select Date Range", [min_date, max_date])

                    start_date, end_date = selected_date
                    df_filtered = df[(df['entity id'] == selected_entity) & (df['misdate'] >= start_date) & (df['misdate'] <= end_date)]

                    order_count = df_filtered['order id'].nunique()
                    fig_order_count = go.Figure(go.Indicator(
                        mode="number",
                        value=order_count,
                        title={"text": "Total Orders"}
                    ))
                    st.plotly_chart(fig_order_count)

                    df_filtered.loc[:, 'Month'] = pd.to_datetime(df_filtered['misdate']).dt.to_period('M').astype(str)
                    month_orders = df_filtered.groupby('Month')['order id'].nunique().reset_index()
                    fig_mom_orders = px.bar(month_orders, x='Month', y='order id', title='Month-on-Month Orders')
                    st.plotly_chart(fig_mom_orders)

                    gender_orders = df_filtered.groupby('gender')['order id'].nunique().reset_index()
                    fig_gender_orders = px.pie(gender_orders, names='gender', values='order id', title='Gender-wise Order Count')
                    st.plotly_chart(fig_gender_orders)

                    age_orders = df_filtered.groupby('age bracket')['order id'].nunique().reset_index()
                    fig_age_orders = px.pie(age_orders, names='age bracket', values='order id', title='Age Bracket Order Count')
                    st.plotly_chart(fig_age_orders)

                    visit_orders = df_filtered.groupby('visit type')['order id'].nunique().reset_index()
                    fig_visit_orders = px.pie(visit_orders, names='visit type', values='order id', title='Visit Type Order Count')
                    st.plotly_chart(fig_visit_orders)

                    relation_orders = df_filtered.groupby('relation')['order id'].nunique().reset_index()
                    fig_relation_orders = px.pie(relation_orders, names='relation', values='order id', title='Relation-wise Order Count')
                    st.plotly_chart(fig_relation_orders)

                else:
                    st.error("No data available for the selected entity and date range.")
            else:
                st.error("The 'entity id' column is missing from the DataFrame.")
        else:
            st.error("Invalid username or password.")
    else:
        st.info("Please enter your username and password to access the dashboard.")

if __name__ == "__main__":
    main()
    
