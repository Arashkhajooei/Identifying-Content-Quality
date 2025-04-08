import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------
# Step 1: Generate Clean Synthetic Data
# -------------------------

np.random.seed(42)
n = 300

# Base features
synthetic_df = pd.DataFrame({
    'image_quality': np.random.uniform(0, 10, n),
    'desc_length': np.random.normal(120, 30, n).clip(20, 250),
    'price_accuracy': np.random.choice([0, 1], size=n, p=[0.3, 0.7])
})

# Content Score = weighted sum of other quality dimensions
synthetic_df['content_score'] = (
    0.3 * synthetic_df['image_quality'] +
    0.3 * (synthetic_df['desc_length'] / 250) * 10 +
    0.4 * synthetic_df['price_accuracy'] * 10
).clip(0, 10)

# Target variables with mild noise
synthetic_df['CTR'] = (0.2 + 0.02 * synthetic_df['content_score'] + np.random.normal(0, 0.02, n)).clip(0, 1)
synthetic_df['ConversionRate'] = (0.1 + 0.03 * synthetic_df['content_score'] + np.random.normal(0, 0.015, n)).clip(0, 1)
synthetic_df['star_rating'] = (3 + 0.2 * synthetic_df['content_score'] + np.random.normal(0, 0.3, n)).clip(1, 5)

# -------------------------
# Step 2: Add Segmenting Columns (City and Category)
# -------------------------

cities = ['Tehran', 'Shiraz', 'Isfahan']
categories = ['Fast Food', 'Traditional', 'Healthy']
synthetic_df['city_name'] = np.random.choice(cities, size=n)
synthetic_df['category'] = np.random.choice(categories, size=n)

# -------------------------
# Step 3: Overall Correlation Analysis
# -------------------------

correlation_matrix = synthetic_df[[
    'image_quality', 'desc_length', 'price_accuracy', 
    'content_score', 'CTR', 'ConversionRate', 'star_rating'
]].corr()

# Heatmap
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="YlGnBu", fmt=".2f")
plt.title("Correlation Between Content KPIs and User Interaction Metrics")
plt.tight_layout()
plt.show()

# -------------------------
# Step 4: Segment-Wise Correlation Analysis (City x Category)
# -------------------------

segment_results = {}

for city in cities:
    for cat in categories:
        subset = synthetic_df[(synthetic_df['city_name'] == city) & (synthetic_df['category'] == cat)]
        if len(subset) > 10:
            corr = subset[['content_score', 'CTR', 'ConversionRate', 'star_rating']].corr().loc['content_score']
            segment_results[(city, cat)] = corr

segment_df = pd.DataFrame(segment_results).T.reset_index()
segment_df.columns = ['city_name', 'category', 'content_corr', 'CTR_corr', 'ConversionRate_corr', 'star_rating_corr']

# View segment correlation results
print("\nSegmented Correlation Results:")
print(segment_df)

# Optional: Heatmap for one city (Tehran)
tehran_df = synthetic_df[synthetic_df['city_name'] == 'Tehran']
plt.figure(figsize=(8, 5))
sns.heatmap(tehran_df[['content_score', 'CTR', 'ConversionRate', 'star_rating']].corr(), annot=True, cmap="coolwarm", fmt=".2f")
plt.title("Correlation Matrix - Tehran")
plt.tight_layout()
plt.show()
