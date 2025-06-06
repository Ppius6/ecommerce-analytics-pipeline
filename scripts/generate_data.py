import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Create data directory
os.makedirs('data', exist_ok=True)

fake = Faker()
np.random.seed(42)
random.seed(42)

# Configuration
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200  
NUM_ORDERS = 5000

def generate_customers(n):
    return pd.DataFrame([{
        'customer_id': i + 1,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'registration_date': fake.date_between(start_date='-2y', end_date='today'),
        'country': fake.country_code(),
        'city': fake.city(),
        'age': random.randint(18, 70),
        'customer_segment': random.choice(['Premium', 'Standard', 'Basic'])
    } for i in range(n)])

def generate_products(n):
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']
    return pd.DataFrame([{
        'product_id': i + 1,
        'product_name': fake.catch_phrase(),
        'category': random.choice(categories),
        'price': round(random.uniform(10.0, 500.0), 2),
        'cost': round(random.uniform(5.0, 300.0), 2),
        'supplier': fake.company(),
        'launch_date': fake.date_between(start_date='-1y', end_date='today')
    } for i in range(n)])

def generate_orders_and_items(n, customers_df, products_df):
    orders = []
    order_items = []
    
    for i in range(n):
        customer = customers_df.sample(1).iloc[0]
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        order_id = i + 1
        
        # Generate order items
        num_items = random.randint(1, 5)
        order_total = 0
        
        for _ in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 3)
            item_total = product['price'] * quantity
            order_total += item_total
            
            order_items.append({
                'order_id': order_id,
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': product['price'],
                'total_amount': item_total
            })
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'order_date': order_date,
            'total_amount': round(order_total, 2),
            'status': random.choice(['completed', 'pending', 'cancelled', 'shipped']),
            'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer']),
            'discount_amount': round(random.uniform(0, order_total * 0.2), 2)
        })
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

if __name__ == "__main__":
    print("🏗️  Generating modern e-commerce dataset...")
    
    customers_df = generate_customers(NUM_CUSTOMERS)
    products_df = generate_products(NUM_PRODUCTS)
    orders_df, order_items_df = generate_orders_and_items(NUM_ORDERS, customers_df, products_df)
    
    # Save datasets
    customers_df.to_csv('data/customers.csv', index=False)
    products_df.to_csv('data/products.csv', index=False) 
    orders_df.to_csv('data/orders.csv', index=False)
    order_items_df.to_csv('data/order_items.csv', index=False)
    
    print(f"✅ Generated:")
    print(f"   • {len(customers_df):,} customers")
    print(f"   • {len(products_df):,} products") 
    print(f"   • {len(orders_df):,} orders")
    print(f"   • {len(order_items_df):,} order items")
    print(f"📁 Data saved to data/ directory")