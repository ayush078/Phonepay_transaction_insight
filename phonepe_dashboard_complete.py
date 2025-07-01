
import os
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Configuration
GITHUB_REPO_URL = "https://github.com/PhonePe/pulse.git"
DATABASE_NAME = "phonepe_data.db"
REPO_DIR = "pulse"

def install_requirements():
    """Install required packages if not already installed"""
    required_packages = [
        'streamlit',
        'plotly',
        'pandas',
        'gitpython'
    ]
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def clone_phonepe_repository():
    """Clone the PhonePe Pulse repository if it doesn't exist"""
    if not os.path.exists(REPO_DIR):
        print("Cloning PhonePe Pulse repository...")
        try:
            subprocess.run(['git', 'clone', GITHUB_REPO_URL, REPO_DIR], check=True)
            print("Repository cloned successfully!")
        except subprocess.CalledProcessError:
            print("Error: Failed to clone repository. Please ensure git is installed.")
            return False
        except FileNotFoundError:
            print("Error: Git is not installed. Please install git first.")
            return False
    else:
        print("Repository already exists.")
    return True

def process_aggregated_transaction_data(base_path):
    """Process aggregated transaction data"""
    data = []
    path = os.path.join(base_path, 'data', 'aggregated', 'transaction', 'country', 'india', 'state')
    
    if not os.path.exists(path):
        print(f"Path not found: {path}")
        return pd.DataFrame()
    
    states = os.listdir(path)
    print(f"Processing {len(states)} states for aggregated transaction data...")
    
    for state in states:
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        years = os.listdir(state_path)
        for year in years:
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter_path = os.path.join(year_path, quarter_file)
                    try:
                        with open(quarter_path, 'r') as f:
                            d = json.load(f)
                            if 'data' in d and 'transactionData' in d['data']:
                                for transaction in d['data']['transactionData']:
                                    if 'paymentInstruments' in transaction and transaction['paymentInstruments']:
                                        data.append({
                                            'state': state,
                                            'year': int(year),
                                            'quarter': int(quarter_file.split('.')[0]),
                                            'transaction_type': transaction['name'],
                                            'transaction_count': transaction['paymentInstruments'][0]['count'],
                                            'transaction_amount': transaction['paymentInstruments'][0]['amount']
                                        })
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        print(f"Error processing {quarter_path}: {e}")
                        continue
    
    return pd.DataFrame(data)

def process_aggregated_user_data(base_path):
    """Process aggregated user data"""
    data = []
    path = os.path.join(base_path, 'data', 'aggregated', 'user', 'country', 'india', 'state')
    
    if not os.path.exists(path):
        return pd.DataFrame()
    
    states = os.listdir(path)
    print(f"Processing {len(states)} states for aggregated user data...")
    
    for state in states:
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        years = os.listdir(state_path)
        for year in years:
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter_path = os.path.join(year_path, quarter_file)
                    try:
                        with open(quarter_path, 'r') as f:
                            d = json.load(f)
                            if 'data' in d and 'aggregated' in d['data']:
                                registered_users = d['data']['aggregated']['registeredUsers']
                                app_opens = d['data']['aggregated']['appOpens']
                                
                                if 'usersByDevice' in d['data'] and d['data']['usersByDevice']:
                                    for device_data in d['data']['usersByDevice']:
                                        data.append({
                                            'state': state,
                                            'year': int(year),
                                            'quarter': int(quarter_file.split('.')[0]),
                                            'registered_users': registered_users,
                                            'app_opens': app_opens,
                                            'brand': device_data['brand'],
                                            'count': device_data['count'],
                                            'percentage': device_data['percentage']
                                        })
                                else:
                                    data.append({
                                        'state': state,
                                        'year': int(year),
                                        'quarter': int(quarter_file.split('.')[0]),
                                        'registered_users': registered_users,
                                        'app_opens': app_opens,
                                        'brand': 'Other',
                                        'count': 0,
                                        'percentage': 0
                                    })
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        print(f"Error processing {quarter_path}: {e}")
                        continue
    
    return pd.DataFrame(data)

def process_map_transaction_data(base_path):
    """Process map transaction data"""
    data = []
    path = os.path.join(base_path, 'data', 'map', 'transaction', 'hover', 'country', 'india', 'state')
    
    if not os.path.exists(path):
        return pd.DataFrame()
    
    states = os.listdir(path)
    print(f"Processing {len(states)} states for map transaction data...")
    
    for state in states:
        state_path = os.path.join(path, state)
        if not os.path.isdir(state_path):
            continue
            
        years = os.listdir(state_path)
        for year in years:
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue
                
            for quarter_file in os.listdir(year_path):
                if quarter_file.endswith('.json'):
                    quarter_path = os.path.join(year_path, quarter_file)
                    try:
                        with open(quarter_path, 'r') as f:
                            d = json.load(f)
                            if 'data' in d and 'hoverDataList' in d['data']:
                                for district in d['data']['hoverDataList']:
                                    if 'metric' in district and district['metric']:
                                        data.append({
                                            'state': state,
                                            'year': int(year),
                                            'quarter': int(quarter_file.split('.')[0]),
                                            'district': district['name'],
                                            'transaction_count': district['metric'][0]['count'],
                                            'transaction_amount': district['metric'][0]['amount']
                                        })
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        print(f"Error processing {quarter_path}: {e}")
                        continue
    
    return pd.DataFrame(data)

def setup_database():
    """Set up SQLite database with processed data"""
    print("Setting up database...")
    
    # Check if repository exists
    if not os.path.exists(REPO_DIR):
        print("Repository not found. Please run the data setup first.")
        return False
    
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    try:
        # Process and load aggregated transaction data
        print("Processing aggregated transaction data...")
        df_agg_trans = process_aggregated_transaction_data(REPO_DIR)
        if not df_agg_trans.empty:
            df_agg_trans.to_sql("aggregated_transaction", conn, if_exists="replace", index=False)
            print(f"Loaded {len(df_agg_trans)} aggregated transaction records")
        
        # Process and load aggregated user data
        print("Processing aggregated user data...")
        df_agg_user = process_aggregated_user_data(REPO_DIR)
        if not df_agg_user.empty:
            df_agg_user.to_sql("aggregated_user", conn, if_exists="replace", index=False)
            print(f"Loaded {len(df_agg_user)} aggregated user records")
        
        # Process and load map transaction data
        print("Processing map transaction data...")
        df_map_trans = process_map_transaction_data(REPO_DIR)
        if not df_map_trans.empty:
            df_map_trans.to_sql("map_transaction", conn, if_exists="replace", index=False)
            print(f"Loaded {len(df_map_trans)} map transaction records")
        
        conn.close()
        print("Database setup completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        conn.close()
        return False

@st.cache_data
def get_data_from_db(query):
    """Get data from database with caching"""
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def create_dashboard():
    """Create the Streamlit dashboard"""
    
    # Set page configuration
    st.set_page_config(
        page_title="PhonePe Transaction Insights",
        page_icon="📱",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
        .main-header {
            font-size: 3rem;
            color: #5d2a8a;
            text-align: center;
            margin-bottom: 2rem;
            font-weight: bold;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 10px;
            border-left: 5px solid #5d2a8a;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # Check if database exists
    if not os.path.exists(DATABASE_NAME):
        st.error("Database not found! Please run the setup first.")
        st.info("Run the following in your terminal: `python phonepe_dashboard_complete.py setup`")
        return
    
    # Main title
    st.markdown('<h1 class="main-header">📱 PhonePe Transaction Insights Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar for filters
    st.sidebar.header("🔍 Filters")
    
    # Get available data for filters
    try:
        years_query = "SELECT DISTINCT year FROM aggregated_transaction ORDER BY year"
        years_df = get_data_from_db(years_query)
        years = years_df['year'].tolist() if not years_df.empty else [2024]
        
        quarters_query = "SELECT DISTINCT quarter FROM aggregated_transaction ORDER BY quarter"
        quarters_df = get_data_from_db(quarters_query)
        quarters = quarters_df['quarter'].tolist() if not quarters_df.empty else [1, 2, 3, 4]
        
        states_query = "SELECT DISTINCT state FROM aggregated_transaction ORDER BY state"
        states_df = get_data_from_db(states_query)
        states = states_df['state'].tolist() if not states_df.empty else []
        
    except Exception as e:
        st.error(f"Error loading filter data: {e}")
        return
    
    # Sidebar filters
    selected_year = st.sidebar.selectbox("Select Year", years, index=len(years)-1 if years else 0)
    selected_quarter = st.sidebar.selectbox("Select Quarter", quarters, index=len(quarters)-1 if quarters else 0)
    selected_states = st.sidebar.multiselect("Select States", states, default=states[:5] if len(states) >= 5 else states)
    
    # Main dashboard content
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "💳 Transactions", "👥 Users"])
    
    with tab1:
        st.header("📊 Overview Dashboard")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        # Total transactions
        total_trans_query = f"""
        SELECT 
            COALESCE(SUM(transaction_amount), 0) as total_amount, 
            COALESCE(SUM(transaction_count), 0) as total_count
        FROM aggregated_transaction
        WHERE year = {selected_year} AND quarter = {selected_quarter}
        """
        total_trans_df = get_data_from_db(total_trans_query)
        
        with col1:
            if not total_trans_df.empty and total_trans_df['total_amount'].iloc[0] is not None:
                amount = total_trans_df['total_amount'].iloc[0]
                st.metric(
                    label="Total Transaction Amount",
                    value=f"₹{amount:,.0f}",
                    delta="Current Quarter"
                )
            else:
                st.metric(label="Total Transaction Amount", value="₹0", delta="No data")
        
        with col2:
            if not total_trans_df.empty and total_trans_df['total_count'].iloc[0] is not None:
                count = total_trans_df['total_count'].iloc[0]
                st.metric(
                    label="Total Transactions",
                    value=f"{count:,.0f}",
                    delta="Current Quarter"
                )
            else:
                st.metric(label="Total Transactions", value="0", delta="No data")
        
        # Total users
        total_users_query = f"""
        SELECT 
            COALESCE(SUM(registered_users), 0) as total_users, 
            COALESCE(SUM(app_opens), 0) as total_opens
        FROM aggregated_user
        WHERE year = {selected_year} AND quarter = {selected_quarter}
        """
        total_users_df = get_data_from_db(total_users_query)
        
        with col3:
            if not total_users_df.empty and total_users_df['total_users'].iloc[0] is not None:
                users = total_users_df['total_users'].iloc[0]
                st.metric(
                    label="Total Registered Users",
                    value=f"{users:,.0f}",
                    delta="Current Quarter"
                )
            else:
                st.metric(label="Total Registered Users", value="0", delta="No data")
        
        with col4:
            if not total_users_df.empty and total_users_df['total_opens'].iloc[0] is not None:
                opens = total_users_df['total_opens'].iloc[0]
                st.metric(
                    label="Total App Opens",
                    value=f"{opens:,.0f}",
                    delta="Current Quarter"
                )
            else:
                st.metric(label="Total App Opens", value="0", delta="No data")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Transaction trend over time
            trend_query = """
            SELECT year, quarter, SUM(transaction_amount) as total_amount
            FROM aggregated_transaction
            GROUP BY year, quarter
            ORDER BY year, quarter
            """
            trend_df = get_data_from_db(trend_query)
            
            if not trend_df.empty:
                trend_df['period'] = trend_df['year'].astype(str) + '-Q' + trend_df['quarter'].astype(str)
                fig_trend = px.line(trend_df, x='period', y='total_amount', 
                                   title='Transaction Amount Trend Over Time',
                                   labels={'total_amount': 'Transaction Amount (₹)', 'period': 'Period'})
                fig_trend.update_layout(height=400)
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info("No trend data available")
        
        with col2:
            # Top transaction types
            trans_type_query = f"""
            SELECT transaction_type, SUM(transaction_amount) as total_amount
            FROM aggregated_transaction
            WHERE year = {selected_year} AND quarter = {selected_quarter}
            GROUP BY transaction_type
            ORDER BY total_amount DESC
            LIMIT 10
            """
            trans_type_df = get_data_from_db(trans_type_query)
            
            if not trans_type_df.empty:
                fig_pie = px.pie(trans_type_df, values='total_amount', names='transaction_type',
                                title='Transaction Amount by Type')
                fig_pie.update_layout(height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No transaction type data available")
    
    with tab2:
        st.header("💳 Transaction Analysis")
        
        # State-wise transaction analysis
        if selected_states:
            state_filter = "'" + "','".join(selected_states) + "'"
            state_trans_query = f"""
            SELECT state, 
                   COALESCE(SUM(transaction_amount), 0) as total_amount, 
                   COALESCE(SUM(transaction_count), 0) as total_count
            FROM aggregated_transaction
            WHERE year = {selected_year} AND quarter = {selected_quarter} AND state IN ({state_filter})
            GROUP BY state
            ORDER BY total_amount DESC
            """
            state_trans_df = get_data_from_db(state_trans_query)
            
            col1, col2 = st.columns(2)
            
            with col1:
                if not state_trans_df.empty:
                    fig_bar = px.bar(state_trans_df, x='state', y='total_amount',
                                   title='Transaction Amount by State',
                                   labels={'total_amount': 'Transaction Amount (₹)', 'state': 'State'})
                    fig_bar.update_layout(height=400)
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.info("No state transaction data available")
            
            with col2:
                if not state_trans_df.empty:
                    fig_bar2 = px.bar(state_trans_df, x='state', y='total_count',
                                    title='Transaction Count by State',
                                    labels={'total_count': 'Transaction Count', 'state': 'State'})
                    fig_bar2.update_layout(height=400)
                    st.plotly_chart(fig_bar2, use_container_width=True)
                else:
                    st.info("No state transaction count data available")
        
        # Top districts by transaction amount
        st.subheader("🏆 Top Districts by Transaction Amount")
        top_districts_query = f"""
        SELECT state, district, SUM(transaction_amount) as total_amount
        FROM map_transaction
        WHERE year = {selected_year} AND quarter = {selected_quarter}
        GROUP BY state, district
        ORDER BY total_amount DESC
        LIMIT 15
        """
        top_districts_df = get_data_from_db(top_districts_query)
        
        if not top_districts_df.empty:
            fig_top = px.bar(top_districts_df, x='district', y='total_amount',
                            color='state', title='Top 15 Districts by Transaction Amount',
                            labels={'total_amount': 'Transaction Amount (₹)', 'district': 'District'})
            fig_top.update_layout(height=500, xaxis_tickangle=-45)
            st.plotly_chart(fig_top, use_container_width=True)
        else:
            st.info("No district transaction data available")
    
    with tab3:
        st.header("👥 User Analysis")
        
        # User metrics by state
        if selected_states:
            state_filter = "'" + "','".join(selected_states) + "'"
            state_users_query = f"""
            SELECT state, 
                   COALESCE(SUM(registered_users), 0) as total_users, 
                   COALESCE(SUM(app_opens), 0) as total_opens
            FROM aggregated_user
            WHERE year = {selected_year} AND quarter = {selected_quarter} AND state IN ({state_filter})
            GROUP BY state
            ORDER BY total_users DESC
            """
            state_users_df = get_data_from_db(state_users_query)
            
            col1, col2 = st.columns(2)
            
            with col1:
                if not state_users_df.empty:
                    fig_users = px.bar(state_users_df, x='state', y='total_users',
                                     title='Registered Users by State',
                                     labels={'total_users': 'Registered Users', 'state': 'State'})
                    fig_users.update_layout(height=400)
                    st.plotly_chart(fig_users, use_container_width=True)
                else:
                    st.info("No user data available")
            
            with col2:
                if not state_users_df.empty:
                    fig_opens = px.bar(state_users_df, x='state', y='total_opens',
                                     title='App Opens by State',
                                     labels={'total_opens': 'App Opens', 'state': 'State'})
                    fig_opens.update_layout(height=400)
                    st.plotly_chart(fig_opens, use_container_width=True)
                else:
                    st.info("No app opens data available")
        
        # Device brand analysis
        st.subheader("📱 Device Brand Analysis")
        brand_query = f"""
        SELECT brand, SUM(count) as total_count
        FROM aggregated_user
        WHERE year = {selected_year} AND quarter = {selected_quarter} AND brand != 'Other' AND brand IS NOT NULL
        GROUP BY brand
        ORDER BY total_count DESC
        LIMIT 10
        """
        brand_df = get_data_from_db(brand_query)
        
        if not brand_df.empty:
            fig_brand = px.pie(brand_df, values='total_count', names='brand',
                              title='User Distribution by Device Brand')
            fig_brand.update_layout(height=500)
            st.plotly_chart(fig_brand, use_container_width=True)
        else:
            st.info("No device brand data available")
    
    # Footer
    st.markdown("---")
    st.markdown("**PhonePe Transaction Insights Dashboard** | Built with Streamlit and Plotly")

def main():
    """Main function to handle different modes"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "setup":
            print("=== PhonePe Dashboard Setup ===")
            print("Installing required packages...")
            install_requirements()
            
            print("Cloning repository...")
            if clone_phonepe_repository():
                print("Setting up database...")
                if setup_database():
                    print("\n✅ Setup completed successfully!")
                    print("Now run: streamlit run phonepe_dashboard_complete.py")
                else:
                    print("\n❌ Database setup failed!")
            else:
                print("\n❌ Repository setup failed!")
        else:
            print("Usage: python phonepe_dashboard_complete.py setup")
    else:
        # Run Streamlit dashboard
        create_dashboard()

if __name__ == "__main__":
    main()

