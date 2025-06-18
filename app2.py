# Import necessary libraries
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
            u.corporatename,
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
            u.providername AS "provider name",
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
        df['misdate'] = pd.to_datetime(df['misdate'])  # Keep as datetime
        cursor.close()
        connection.close()
        print("Connection closed.")
        return df

    except Exception as e:
        print("Error connecting to Redshift:", e)
        return pd.DataFrame()

# Streamlit app
def main():
    st.set_page_config(layout="wide")  # Use wide layout

    # Toggle for dark/normal mode
    dark_mode = st.sidebar.checkbox("Dark Mode", value=False)
    if dark_mode:
        st.markdown(
            """
            <style>
            .stApp {
                background-color: #333;
                color: #fff;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            """
            <style>
            .stApp {
                background-color: #fff;
                color: #000;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

    # Session state to manage login
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    if not st.session_state.logged_in:
        st.sidebar.title("Login")
        input_username = st.sidebar.text_input("Username", key="input_username")
        input_password = st.sidebar.text_input("Password", type="password", key="input_password")

        if input_username and input_password:
            expected_password = f"{input_username}_medibuddy"
            if input_password == expected_password:
                st.session_state.logged_in = True
                st.session_state.user = input_username
            else:
                st.sidebar.error("Invalid username or password.")
    else:
        st.sidebar.button("Logout", on_click=lambda: st.session_state.update({'logged_in': False, 'user': None}))

    if st.session_state.logged_in:
        df = fetch_data()

        # Filter the DataFrame for the logged-in user's entity id
        user_data = df[df['entity id'] == int(st.session_state.user)]
        if not user_data.empty:
            corporatename = user_data['corporatename'].iloc[0]  # Get the corporate name for the user

            st.title(f"HRWIN Dashboard for {corporatename}")
            st.success(f"Welcome, HR representative for {corporatename}")

            # Filters at the top
            min_date = user_data['misdate'].min().date()
            max_date = user_data['misdate'].max().date()
            selected_date = st.date_input("Select Date Range", [min_date, max_date], key="filter_date_range")
            time_grain = st.selectbox("Select Time Grain", ["Hour", "Day", "Week", "Month", "Year"], key="filter_time_grain")

            start_date, end_date = selected_date
            df_filtered = user_data[(user_data['misdate'] >= pd.to_datetime(start_date)) & (user_data['misdate'] <= pd.to_datetime(end_date))]

            # Aggregate data based on the selected time grain
            if time_grain == "Hour":
                df_filtered['Time'] = df_filtered['misdate'].dt.floor('H')
            elif time_grain == "Day":
                df_filtered['Time'] = df_filtered['misdate'].dt.floor('D')
            elif time_grain == "Week":
                df_filtered['Time'] = df_filtered['misdate'].dt.to_period('W').dt.strftime('%Y-%m-%d')
            elif time_grain == "Month":
                df_filtered['Time'] = df_filtered['misdate'].dt.to_period('M').dt.strftime('%B %Y')
            elif time_grain == "Year":
                df_filtered['Time'] = df_filtered['misdate'].dt.to_period('Y').dt.strftime('%Y')

            # Layout for the dashboard
            col1, col2 = st.columns([2, 1])
            with col1:
                # Total Orders
                order_count = df_filtered['order id'].nunique()
                fig_order_count = go.Figure(go.Indicator(
                    mode="number",
                    value=order_count,
                    title={"text": "Total Orders"}
                ))
                st.plotly_chart(fig_order_count, use_container_width=True)

                # Month-on-Month Orders
                time_orders = df_filtered.groupby('Time')['order id'].nunique().reset_index()
                fig_time_orders = px.bar(time_orders, x='Time', y='order id', title=f'Orders by {time_grain}', text='order id')
                fig_time_orders.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                fig_time_orders.update_layout(xaxis_title=time_grain, yaxis_title='Number of Orders')
                st.plotly_chart(fig_time_orders, use_container_width=True)

            # Row for pie charts
            col1, col2, col3 = st.columns(3)
            with col1:
                gender_orders = df_filtered.groupby('gender')['order id'].nunique().reset_index()
                fig_gender_orders = px.pie(gender_orders, names='gender', values='order id', title='Gender-wise Order Count', color_discrete_sequence=px.colors.qualitative.Plotly)
                st.plotly_chart(fig_gender_orders, use_container_width=True)
            with col2:
                age_orders = df_filtered.groupby('age bracket')['order id'].nunique().reset_index()
                fig_age_orders = px.pie(age_orders, names='age bracket', values='order id', title='Age Bracket Order Count', color_discrete_sequence=px.colors.qualitative.Plotly)
                st.plotly_chart(fig_age_orders, use_container_width=True)
            with col3:
                visit_orders = df_filtered.groupby('visit type')['order id'].nunique().reset_index()
                fig_visit_orders = px.pie(visit_orders, names='visit type', values='order id', title='Visit Type Order Count', color_discrete_sequence=px.colors.qualitative.Plotly)
                st.plotly_chart(fig_visit_orders, use_container_width=True)

            # Row for tables
            col1, col2, col3 = st.columns(3)
            with col1:
                city_orders = df_filtered.groupby('city')['order id'].nunique().reset_index().sort_values(by='order id', ascending=False)
                st.write("City-wise Order Count")
                st.dataframe(city_orders)
            with col2:
                state_orders = df_filtered.groupby('state')['order id'].nunique().reset_index().sort_values(by='order id', ascending=False)
                st.write("State-wise Order Count")
                st.dataframe(state_orders)
            with col3:
                package_orders = df_filtered.groupby('package name')['order id'].nunique().reset_index().sort_values(by='order id', ascending=False)
                st.write("Package-wise Order Count")
                st.dataframe(package_orders)

            # Provider-wise Order Count Bar Chart
            provider_orders = df_filtered.groupby('provider name')['order id'].nunique().reset_index().sort_values(by='order id', ascending=False)
            fig_provider_orders = px.bar(provider_orders, x='provider name', y='order id', title='Provider-wise Order Count', text='order id')
            fig_provider_orders.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig_provider_orders.update_layout(xaxis_title='Provider Name', yaxis_title='Number of Orders')
            st.plotly_chart(fig_provider_orders, use_container_width=True)

        else:
            st.error("No data available for the selected entity id.")
    else:
        st.title("HRWIN Dashboard")
        st.info("Please log in to view the dashboard.")

if __name__ == "__main__":
    main()
