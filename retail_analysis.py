import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#customer dataset
customers= pd.DataFrame({
    "CustomerID":[1,2,3,4,5,6,7,8],
    "CustomerName":[
        "Ahmed","Sara","Omar","Lina","Youssef","Maya","Karim","Nour"
    ],
    "City":[
        "Cairo","Alexandria","Cairo","Giza",
        "Alexandria","Cairo","Giza","Cairo"
    ]
})
#Products dataset
products=pd.DataFrame({
    "ProductID":[101,102,103,104,105],
    "Product":[
        "Laptop","Phone","Tablet","Headphones","Smartwatch"
    ],
    "Category":[
        "Electronics","Electronics","Electronics",
        "Accessories","Accessories"
    ],
    "Price":[1200,800,600,150,250]
})
#Orders dataset
orders=pd.DataFrame({
    "OrderID":[1,2,3,4,5,6,7,8,9,10,11,12],
    "CustomerID":[1,2,3,1,4,5,6,7,8,2,3,5],
    "ProductID":[101,102,103,102,104,105,101,103,102,101,105,104],
    "Quantity":[1,2,1,3,2,1,1,2,2,1,3,1],
    "OrderDate":[
        "2024-01-05","2024-01-12","2024-02-02","2024-02-15",
        "2024-03-01","2024-03-10","2024-03-22","2024-04-05",
        "2024-04-12","2024-04-25","2024-05-03","2024-05-18"
    ]
})

print(customers)
print(products)
print(orders)

orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])
#merging datasets
orders_products=pd.merge(orders, products, on="ProductID")
full_data=pd.merge(orders_products, customers, on="CustomerID")
full_data["TotalAmount"]=full_data["Quantity"]*full_data["Price"]
full_data["Month"]=full_data["OrderDate"].dt.month
full_data["Year"]=full_data["OrderDate"].dt.year
print(full_data)

#cleaning data
full_data.info()
print(full_data.isnull().sum())
print(full_data.describe(include='all'))

print(full_data.dtypes)
print(full_data.isnull().sum())

print(full_data.duplicated(subset=['OrderID', 'CustomerID', 'ProductID']).sum())

print("Quantity <= 0:")
print(full_data[full_data['Quantity'] <= 0])
print("Price <= 0:")
print(full_data[full_data['Price'] <= 0])
print("Future dates:")
print("future dates:")
print(full_data[full_data['OrderDate'] > pd.Timestamp.today()])
print("Cities:", full_data['City'].unique())
print("Products:", full_data['Product'].unique())
print("Categories:", full_data['Category'].unique())
print("Customer Names:", full_data['CustomerName'].unique())

plt.boxplot(full_data['Quantity'])
plt.title('Boxplot of Quantity')
plt.show()
plt.boxplot(full_data['Price'])
plt.title('Boxplot of Price')
plt.show()

#Exploratory Data Analysis
revenue_per_product=full_data.groupby("Product")["TotalAmount"].sum().sort_values(ascending=False)
print("Revenue per Product:")
print(revenue_per_product)


revenue_per_city=full_data.groupby("City")["TotalAmount"].sum().sort_values(ascending=False)
print("Revenue per City:")
print(revenue_per_city)

revenue_per_category=full_data.groupby("Category")["TotalAmount"].sum().sort_values(ascending=False)
print("Revenue per Category:")
print(revenue_per_category)

revenue_per_month=full_data.groupby("Month")["TotalAmount"].sum().sort_values(ascending=False)
print("Revenue per Month:")
print(revenue_per_month)

#visualization
revenue_per_product.plot(kind='bar')
plt.title('Revenue per Product')
plt.xlabel('Product')
plt.ylabel('Revenue')
plt.show()  

revenue_per_city.plot(kind='bar')
plt.title('Revenue per City')
plt.xlabel('City')
plt.ylabel('Revenue')
plt.show()

revenue_per_month.plot(kind='line', marker='o') 
plt.title('Revenue per Month')
plt.xlabel('Month')
plt.ylabel('Revenue')
plt.show()

#sql

import sqlite3
conn = sqlite3.connect('retail_portfolio.db')

customers.to_sql('Customers', conn, if_exists='replace', index=False)
products.to_sql('Products', conn, if_exists='replace', index=False)
orders.to_sql('Orders', conn, if_exists='replace', index=False)

print("Data inserted into SQLite database successfully.")
conn.close()

conn = sqlite3.connect('retail_portfolio.db')
query = """
SELECT p.Product, SUM(o.Quantity * p.Price) as TotalRevenue
FROM products p
JOIN orders o ON p.ProductID = o.ProductID
GROUP BY p.Product
ORDER BY TotalRevenue DESC
"""
product_revenue = pd.read_sql_query(query, conn)
print("Revenue per Product:")
print(product_revenue)
conn.close()

conn = sqlite3.connect('retail_portfolio.db')
query = """
SELECT c.CustomerName, SUM(o.Quantity * p.Price) as TotalRevenue
FROM customers c
JOIN orders o ON c.CustomerID = o.CustomerID
JOIN products p ON o.ProductID = p.ProductID
GROUP BY c.CustomerName
ORDER BY TotalRevenue DESC
LIMIT 3
"""
top_customers = pd.read_sql_query(query, conn)
print("Top 3 Customers by Revenue:")
print(top_customers)
conn.close()

conn = sqlite3.connect('retail_portfolio.db')
query = """
SELECT c.City, SUM(o.Quantity * p.Price) as TotalRevenue
FROM customers c
JOIN orders o ON c.CustomerID = o.CustomerID
JOIN products p ON o.ProductID = p.ProductID
GROUP BY c.City
ORDER BY TotalRevenue DESC
"""
city_revenue = pd.read_sql_query(query, conn)
print("Revenue per City:")
print(city_revenue)
conn.close()

conn = sqlite3.connect('retail_portfolio.db')
query = """
SELECT o.OrderID , SUM(o.Quantity * p.Price) as TotalRevenue
FROM orders o
JOIN products p ON o.ProductID = p.ProductID
GROUP BY o.OrderID
ORDER BY TotalRevenue DESC
LIMIT 1
"""
largest_order = pd.read_sql_query(query, conn)
print("Largest Order by Revenue:")
print(largest_order)
conn.close()

conn = sqlite3.connect('retail_portfolio.db')
query = """
SELECT CustomerName , TotalSpent , RANK() OVER (ORDER BY TotalSpent DESC) as SpendingRank
FROM (
SELECT c.CustomerName , SUM(o.Quantity * p.Price) AS TotalSpent
FROM customers c 
JOIN orders o ON c.CustomerID = o.CustomerID
JOIN products p ON o.ProductID = p.ProductID
GROUP BY c.CustomerName
)
"""
customer_ranking=pd.read_sql_query(query,conn)
print("ranking the top customers")
print(customer_ranking)
conn.close

conn = sqlite3.connect('retail_portfolio.db')
query = """
SELECT Month , TotalRevenue , TotalRevenue - LAG(TotalRevenue) OVER (ORDER BY MONTH) AS MoM_Growth
FROM(
SELECT strftime('%m', o.OrderDate) AS Month, SUM(o.Quantity * p.Price) AS TotalRevenue
FROM orders o 
JOIN products p ON o.ProductID = p.ProductID
GROUP BY Month

)
ORDER BY Month
"""
revenue_growth= pd.read_sql_query(query, conn)
print("revenue growth")
print(revenue_growth)
conn.close

full_data.to_csv("full_data.csv",index=False)

