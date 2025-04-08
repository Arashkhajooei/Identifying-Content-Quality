from sqlalchemy import create_engine, text

# 1. Set up connection to Neon PostgreSQL
db_url = "postgresql://neondb_owner:npg_q6BoAsN7XbJv@ep-polished-tooth-a8yeakco-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
engine = create_engine(db_url)

# 2. Define CREATE TABLE statements (one for each table)
create_statements = [
    """
    CREATE TABLE city (
        city_id SERIAL PRIMARY KEY,
        city_name VARCHAR(100)
    );
    """,
    """
    CREATE TABLE vendor (
        vendor_id SERIAL PRIMARY KEY,
        vendor_name VARCHAR(100),
        category VARCHAR(50),
        city_id INTEGER REFERENCES city(city_id)
    );
    """,
    """
    CREATE TABLE food_item (
        item_id SERIAL PRIMARY KEY,
        vendor_id INTEGER REFERENCES vendor(vendor_id),
        item_name VARCHAR(100),
        has_image BOOLEAN,
        image_quality FLOAT,
        desc_length INTEGER,
        has_tags BOOLEAN,
        price_accuracy BOOLEAN,
        listed_price FLOAT,
        expected_price FLOAT,
        content_score FLOAT
    );
    """,
    """
    CREATE TABLE "user" (
        user_id SERIAL PRIMARY KEY,
        age_group VARCHAR(20),
        city_id INTEGER REFERENCES city(city_id),
        platform VARCHAR(20),
        account_type VARCHAR(20)
    );
    """,
    """
    CREATE TABLE review (
        review_id SERIAL PRIMARY KEY,
        item_id INTEGER REFERENCES food_item(item_id),
        user_id INTEGER REFERENCES "user"(user_id),
        review_text TEXT,
        sentiment_score FLOAT,
        star_rating FLOAT,
        review_length INTEGER
    );
    """,
    """
    CREATE TABLE user_interaction (
        interaction_id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES "user"(user_id),
        item_id INTEGER REFERENCES food_item(item_id),
        num_views INTEGER,
        num_clicks INTEGER,
        num_orders INTEGER,
        time_on_page FLOAT,
        added_to_fav BOOLEAN,
        session_length FLOAT,
        position_in_list INTEGER,
        date TIMESTAMP
    );
    """
]

# 3. Execute each CREATE TABLE statement
with engine.connect() as conn:
    for stmt in create_statements:
        conn.execute(text(stmt))
    conn.commit()

print("✅ All tables created successfully in PostgreSQL!")
