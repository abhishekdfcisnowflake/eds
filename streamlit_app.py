import streamlit as st
import snowflake.connector
from datetime import datetime

# ❄️ Snowflake connection (cached for efficiency)
@st.cache_resource
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="UTIL_DB",
            schema="PUBLIC"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Snowflake connection failed: {e}")
        st.stop()

# 🔎 Fetch all databases and schemas
def fetch_databases_and_schemas(conn):
    db_schemas = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            databases = [row[1] for row in cur.fetchall() if row[1] not in ('SNOWFLAKE', 'UTIL_DB')]

            for db in databases:
                try:
                    cur.execute(f"SHOW SCHEMAS IN DATABASE {db}")
                    schemas = [row[1] for row in cur.fetchall()]
                    db_schemas[db] = schemas
                except:
                    db_schemas[db] = []
    except Exception as e:
        st.error(f"⚠️ Failed to fetch database/schema info: {e}")
    return db_schemas

# 📝 Insert access request into tracking table
def insert_access_request(conn, user_email, db_name, schema_name, access_type,
                          manager_name, manager_email, department, purpose):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO UTIL_DB.PUBLIC.ACCESS_REQUESTS (
                    USER_EMAIL, DATABASE_NAME, SCHEMA_NAME, ACCESS_TYPE,
                    MANAGER_NAME, MANAGER_EMAIL, DEPARTMENT, PURPOSE
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (user_email, db_name, schema_name, access_type, manager_name, manager_email, department, purpose)
            )
    except Exception as e:
        st.error(f"❌ Failed to insert request for {db_name}.{schema_name}: {e}")

# 🖥️ Streamlit UI
st.title("🔐 Snowflake Access Request Portal")

with st.form("access_request_form"):
    user_email = st.text_input("Your Email", placeholder="name@example.com")
    manager_name = st.text_input("Manager Name")
    manager_email = st.text_input("Manager Email", placeholder="manager@example.com")
    department = st.text_input("Your Department")
    purpose = st.text_area("Purpose of Data Access")

    st.markdown("---")

    conn = get_snowflake_connection()
    db_schemas = fetch_databases_and_schemas(conn)

    selected_requests = []

    st.subheader("📂 Select Databases and Schemas")

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

    submitted = st.form_submit_button("🚀 Submit Access Request")

if submitted:
    if not user_email or not manager_name or not manager_email or not department or not purpose:
        st.error("Please fill all fields before submitting.")
    elif not selected_requests:
        st.warning("No database/schema selected.")
    else:
        for db, schema, access in selected_requests:
            insert_access_request(conn, user_email, db, schema, access,
                                  manager_name, manager_email, department, purpose)
        st.success("✅ Your access request(s) have been submitted successfully.")
