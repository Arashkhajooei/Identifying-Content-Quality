from sqlalchemy import create_engine, text
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# DB connection
db_url = "postgresql://neondb_owner:npg_q6BoAsN7XbJv@ep-polished-tooth-a8yeakco-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
engine = create_engine(db_url)
fake = Faker()

# Configuration
iranian_cities = ["Tehran", "Mashhad", "Isfahan", "Shiraz", "Tabriz"]
vendor_categories = ["Fast Food", "Traditional", "Healthy", "Bakery", "Italian"]
iranian_foods = ["Ghormeh Sabzi", "Fesenjan", "Zereshk Polo", "Gheimeh", "Kebab", "Dizi"]
fast_foods = ["Burger", "Hot Dog", "Fried Chicken", "Fries"]
italian_foods = ["Pizza", "Pasta", "Lasagna", "Risotto"]
all_foods = iranian_foods + fast_foods + italian_foods

num_vendors = 20
num_users = 50
num_items = 100
num_reviews = 150
num_interactions = 200

# Insert cities
city_df = pd.DataFrame([{"city_name": city} for city in iranian_cities])
city_df.to_sql("city", engine, if_exists="append", index=False)

with engine.connect() as conn:
    city_ids = [row[0] for row in conn.execute(text("SELECT city_id FROM city")).fetchall()]

# Insert vendors
vendor_data = [{
    "vendor_name": fake.company(),
    "category": random.choice(vendor_categories),
    "city_id": random.choice(city_ids)
} for _ in range(num_vendors)]
pd.DataFrame(vendor_data).to_sql("vendor", engine, if_exists="append", index=False)

with engine.connect() as conn:
    vendor_ids = [row[0] for row in conn.execute(text("SELECT vendor_id FROM vendor")).fetchall()]

# Insert users
user_data = [{
    "age_group": random.choice(["18-24", "25-34", "35-44", "45+"]),
    "city_id": random.choice(city_ids),
    "platform": random.choice(["Android", "iOS", "Web"]),
    "account_type": random.choice(["Guest", "Registered", "Premium"])
} for _ in range(num_users)]
pd.DataFrame(user_data).to_sql("user", engine, if_exists="append", index=False)

with engine.connect() as conn:
    user_ids = [row[0] for row in conn.execute(text("SELECT user_id FROM \"user\"")).fetchall()]

# Insert food items
food_data = []
for _ in range(num_items):
    has_image = random.choice([True, False])
    image_quality = round(random.uniform(5, 10), 2) if has_image else 0
    listed_price = round(random.uniform(5, 20), 2)
    expected_price = listed_price + random.choice([-1, 0, 1])
    food_data.append({
        "vendor_id": random.choice(vendor_ids),
        "item_name": random.choice(all_foods),
        "has_image": has_image,
        "image_quality": image_quality,
        "desc_length": random.randint(30, 200),
        "has_tags": random.choice([True, False]),
        "price_accuracy": abs(listed_price - expected_price) < 0.5,
        "listed_price": listed_price,
        "expected_price": expected_price,
        "content_score": round(random.uniform(6, 10), 2)
    })
pd.DataFrame(food_data).to_sql("food_item", engine, if_exists="append", index=False)

with engine.connect() as conn:
    item_ids = [row[0] for row in conn.execute(text("SELECT item_id FROM food_item")).fetchall()]

# Insert reviews
review_data = [{
    "item_id": random.choice(item_ids),
    "user_id": random.choice(user_ids),
    "review_text": fake.sentence(nb_words=10),
    "sentiment_score": round(random.uniform(-1, 1), 2),
    "star_rating": round(random.uniform(2.5, 5.0), 1),
    "review_length": random.randint(10, 50)
} for _ in range(num_reviews)]
pd.DataFrame(review_data).to_sql("review", engine, if_exists="append", index=False)

# Insert user interactions
interaction_data = []
for _ in range(num_interactions):
    views = random.randint(1, 20)
    clicks = random.randint(1, views)
    orders = random.randint(0, clicks)
    interaction_data.append({
        "user_id": random.choice(user_ids),
        "item_id": random.choice(item_ids),
        "num_views": views,
        "num_clicks": clicks,
        "num_orders": orders,
        "time_on_page": round(random.uniform(5, 120), 1),
        "added_to_fav": random.choice([True, False]),
        "session_length": round(random.uniform(30, 300), 1),
        "position_in_list": random.randint(1, 10),
        "date": fake.date_time_between(start_date="-30d", end_date="now")
    })
pd.DataFrame(interaction_data).to_sql("user_interaction", engine, if_exists="append", index=False)

print("✅ All synthetic data inserted successfully into your PostgreSQL Neon DB.")
