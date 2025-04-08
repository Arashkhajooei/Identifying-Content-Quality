# SnappFood Content Quality Analysis - Jupyter Notebook
# Author: Arash Khajooei
# Date: April 7, 2025

# --- 1. SETUP ---
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import psycopg2
from sqlalchemy import create_engine

# DB connection string
DB_URL = "postgresql://neondb_owner:npg_q6BoAsN7XbJv@ep-polished-tooth-a8yeakco-pooler.eastus2.azure.neon.tech/neondb?sslmode=require"
engine = create_engine(DB_URL)

# --- 2. LOAD SYNTHETIC DATA (Assuming table exists) ---
# Replace this with real SQL query if applicable
data = pd.read_sql("SELECT * FROM synthetic_food_items", engine)
data.head()

# --- 3. CONTENT QUALITY SCORE (CQS) CALCULATION ---
def calculate_desc_score(length):
    if length < 100:
        return 0.3
    elif 100 <= length <= 250:
        return 1.0
    elif 250 < length <= 400:
        return 0.6
    else:
        return 0.3

data['desc_score'] = data['desc_length'].apply(calculate_desc_score)

# Normalize image_quality to [0,1] if not already
if data['image_quality'].max() > 1:
    data['image_quality'] = data['image_quality'] / 10.0

# Convert booleans to ints
data['has_image'] = data['has_image'].astype(int)
data['price_accuracy'] = data['price_accuracy'].astype(int)
data['has_tags'] = data['has_tags'].astype(int)

# Calculate CQS
data['CQS'] = (
    0.20 * data['has_image'] +
    0.25 * data['image_quality'] +
    0.20 * data['desc_score'] +
    0.15 * data['has_tags'] +
    0.20 * data['price_accuracy']
)

# --- 4. VISUALIZE CONTENT QUALITY BUCKETS ---
data['CQS_bucket'] = pd.cut(data['CQS'], bins=[0, 0.4, 0.6, 0.8, 1.0], labels=['0-0.4', '0.4-0.6', '0.6-0.8', '0.8-1.0'])

# Group by city and bucket
bucket_summary = data.groupby(['city', 'CQS_bucket'])[['views', 'clicks', 'orders', 'time_on_page']].mean().reset_index()

# Plot clicks by city and CQS bucket
plt.figure(figsize=(12, 6))
sns.barplot(data=bucket_summary, x='CQS_bucket', y='clicks', hue='city')
plt.title('Average Clicks by CQS Bucket and City')
plt.ylabel('Avg Clicks')
plt.xlabel('Content Quality Score Bucket')
plt.legend(title='City')
plt.tight_layout()
plt.show()

# --- 5. PREDICTIVE MODELING ---
features = ['CQS', 'image_quality', 'price_accuracy', 'vendor_rating']
X = data[features]
X['price_accuracy'] = X['price_accuracy'].astype(int)
y_clicks = data['clicks']
y_orders = data['orders']

# Split and model - clicks
X_train, X_test, y_train, y_test = train_test_split(X, y_clicks, test_size=0.2, random_state=42)
rf_clicks = RandomForestRegressor(n_estimators=100, random_state=42)
rf_clicks.fit(X_train, y_train)
preds_clicks = rf_clicks.predict(X_test)
print("R^2 for Clicks:", r2_score(y_test, preds_clicks))

# Model - orders
X_train, X_test, y_train, y_test = train_test_split(X, y_orders, test_size=0.2, random_state=42)
rf_orders = RandomForestRegressor(n_estimators=100, random_state=42)
rf_orders.fit(X_train, y_train)
preds_orders = rf_orders.predict(X_test)
print("R^2 for Orders:", r2_score(y_test, preds_orders))

# Feature Importances
importances_clicks = pd.Series(rf_clicks.feature_importances_, index=features).sort_values(ascending=False)
importances_orders = pd.Series(rf_orders.feature_importances_, index=features).sort_values(ascending=False)

print("\nTop Features for Clicks Prediction:\n", importances_clicks)
print("\nTop Features for Orders Prediction:\n", importances_orders)

# --- 6. SIMULATION: BOOST LOW-QUALITY ITEMS ---
data_simulated = data.copy()
data_simulated.loc[data_simulated['CQS'] < 0.6, 'CQS'] = 0.9

X_simulated = data_simulated[features]
X_simulated['price_accuracy'] = X_simulated['price_accuracy'].astype(int)

predicted_clicks = rf_clicks.predict(X_simulated)
predicted_orders = rf_orders.predict(X_simulated)

data_simulated['clicks_predicted'] = predicted_clicks
data_simulated['orders_predicted'] = predicted_orders

# Compare average lift
lift_clicks = data_simulated['clicks_predicted'].mean() - data['clicks'].mean()
lift_orders = data_simulated['orders_predicted'].mean() - data['orders'].mean()

print(f"\nEstimated average lift in Clicks: {lift_clicks:.2f} per item")
print(f"Estimated average lift in Orders: {lift_orders:.2f} per item")

# --- 7. REAL-TIME ARCHITECTURE SKETCH ---
from IPython.display import Markdown
Markdown("""
**Real-Time Data Stack:**
- **Apache Kafka**: Ingest user interactions and vendor content updates
- **PySpark (Streaming)**: Process and compute content scores in real time
- **Apache Druid**: Real-time analytical storage and sub-second OLAP querying
- **Power BI / Grafana**: Visual dashboards for monitoring performance

This architecture helps drive timely feedback, issue detection, and real-time optimization.
""")
