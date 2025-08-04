import streamlit as st
import snowflake.connector

# ❄️ Snowflake connection (replace with Streamlit Secrets or a secure method)
def get_snowflake_connection():
    return snowflake.connector.connect(
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        account="YOUR_ACCOUNT_ID",
        warehouse="YOUR_WAREHOUSE",
        database="UTIL_DB",
        schema="PUBLIC"
    )

# 🔎 Fetch all databases and schemas
def fetch_databases_and_schemas(conn):
    cur = conn.cursor()
    cur.execute("SHOW DATABASES")
    databases = [row[1] for row in cur.fetchall() if row[1] not in ('SNOWFLAKE', 'UTIL_DB')]

    db_schemas = {}
    for db in databases:
        try:
            cur.execute(f"SHOW SCHEMAS IN DATABASE {db}")
            schemas = [row[1] for row in cur.fetchall()]
            db_schemas[db] = schemas
        except:
            db_schemas[db] = []
    return db_schemas

# 📝 Insert access request into tracking table
def insert_access_request(conn, user_email, manager_name, manager_email, department, purpose, db_name, schema_name, access_type):
    cur = conn.cursor()
    insert_query = f"""
        INSERT INTO UTIL_DB.PUBLIC.ACCESS_REQUESTS (
            USER_EMAIL, MANAGER_NAME, MANAGER_EMAIL, DEPARTMENT, PURPOSE, 
            DATABASE_NAME, SCHEMA_NAME, ACCESS_TYPE
        )
        VALUES (
            '{user_email}', '{manager_name}', '{manager_email}', '{department}', '{purpose}',
            '{db_name}', '{schema_name}', '{access_type}'
        )
    """
    cur.execute(insert_query)
    cur.close()

# 🚀 Streamlit App UI
st.title("🔐 Snowflake Access Request Portal")

# Open the form block
with st.form("access_request_form"):
    st.subheader("🔧 Your Information")
    user_email = st.text_input("Your Email", placeholder="name@example.com")
    manager_name = st.text_input("Manager Name")
    manager_email = st.text_input("Manager Email")
    department = st.text_input("Department")
    purpose = st.text_area("Purpose of Data Access")

    st.subheader("📂 Database Access Request")
    selected_requests = []

    try:
        conn = get_snowflake_connection()
        db_schemas = fetch_databases_and_schemas(conn)

        for db, schemas in db_schemas.items():
            with st.expander(f"📁 {db}", expanded=False):
                for schema in schemas:
                    col1, col2 = st.columns([3, 2])
                    with col1:
                        checkbox = st.checkbox(f"{db}.{schema}", key=f"{db}_{schema}")
                    with col2:
                        access_type = st.selectbox(
                            f"Access Type for {db}.{schema}", 
                            ['READ', 'WRITE'], 
                            key=f"{db}_{schema}_access"
                        )
                    if checkbox:
                        selected_requests.append((db, schema, access_type))
    except Exception as e:
        st.error("Error connecting to Snowflake. Please contact admin.")
        st.stop()

    # Submit button
    submitted = st.form_submit_button("🚀 Submit Access Request")

    if submitted:
        if not user_email or not manager_name or not manager_email or not department or not purpose:
            st.error("Please fill in all required fields.")
        elif not selected_requests:
            st.warning("Please select at least one schema to request access.")
        else:
            try:
                for db, schema, access in selected_requests:
                    insert_access_request(
                        conn, user_email, manager_name, manager_email, department, purpose,
                        db, schema, access
                    )
                st.success("✅ Your access request(s) have been submitted successfully.")
            except Exception as e:
                st.error(f"❌ Failed to submit request: {e}")
