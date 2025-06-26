#!/usr/bin/env python3
"""
Database Forge - SQLAlchemy ORM with SQLite
Creates Category and Product tables, populates with sample data, and retrieves results.
"""

import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create base class for declarative models
Base = declarative_base()

class Category(Base):
    """Category table model"""
    __tablename__ = 'categories'
    
    category_id = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(100), nullable=False, unique=True)
    
    # Relationship to products
    products = relationship("Product", back_populates="category")
    
    def __repr__(self):
        return f"<Category(id={self.category_id}, name='{self.category_name}')>"

class Product(Base):
    """Product table model"""
    __tablename__ = 'products'
    
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(200), nullable=False)
    price = Column(Float, nullable=False)
    category_id = Column(Integer, ForeignKey('categories.category_id'), nullable=False)
    
    # Relationship to category
    category = relationship("Category", back_populates="products")
    
    def __repr__(self):
        return f"<Product(id={self.product_id}, name='{self.product_name}', price=${self.price})>"

def create_database():
    """Create SQLite database and tables"""
    # Create engine for SQLite database
    engine = create_engine('sqlite:///database_forge.db', echo=False)
    
    # Create all tables
    Base.metadata.create_all(engine)
    logger.info("Database and tables created successfully")
    
    return engine

def populate_sample_data(session):
    """Populate tables with sample data"""
    logger.info("Populating sample data...")
    
    # Sample categories
    categories_data = [
        "Electronics",
        "Clothing",
        "Books",
        "Home & Garden",
        "Sports & Outdoors"
    ]
    
    # Create category objects
    categories = []
    for cat_name in categories_data:
        category = Category(category_name=cat_name)
        categories.append(category)
        session.add(category)
    
    # Commit categories first to get their IDs
    session.commit()
    logger.info(f"Added {len(categories)} categories")
    
    # Sample products data (product_name, price, category_name)
    products_data = [
        ("Smartphone", 699.99, "Electronics"),
        ("Laptop", 1299.99, "Electronics"),
        ("Wireless Headphones", 149.99, "Electronics"),
        ("Smart TV", 599.99, "Electronics"),
        ("T-Shirt", 19.99, "Clothing"),
        ("Jeans", 59.99, "Clothing"),
        ("Running Shoes", 89.99, "Clothing"),
        ("Winter Jacket", 129.99, "Clothing"),
        ("Python Programming Book", 39.99, "Books"),
        ("Data Science Handbook", 49.99, "Books"),
        ("Science Fiction Novel", 14.99, "Books"),
        ("Garden Hose", 24.99, "Home & Garden"),
        ("Lawn Mower", 299.99, "Home & Garden"),
        ("Plant Pot Set", 34.99, "Home & Garden"),
        ("Basketball", 29.99, "Sports & Outdoors"),
        ("Camping Tent", 159.99, "Sports & Outdoors"),
        ("Yoga Mat", 24.99, "Sports & Outdoors")
    ]
    
    # Create a mapping of category names to category objects
    category_map = {cat.category_name: cat for cat in categories}
    
    # Create product objects
    products = []
    for prod_name, price, cat_name in products_data:
        product = Product(
            product_name=prod_name,
            price=price,
            category_id=category_map[cat_name].category_id
        )
        products.append(product)
        session.add(product)
    
    session.commit()
    logger.info(f"Added {len(products)} products")

def retrieve_and_display_data(session):
    """Retrieve and display data from the database"""
    logger.info("Retrieving and displaying data...")
    print("\n" + "="*80)
    print("DATABASE FORGE - PRODUCT CATALOG")
    print("="*80)
    
    # Query all categories
    categories = session.query(Category).all()
    print(f"\nTotal Categories: {len(categories)}")
    print("-" * 40)
    for category in categories:
        print(f"ID: {category.category_id:2d} | Category: {category.category_name}")
    
    print("\n" + "="*80)
    print("PRODUCTS BY CATEGORY")
    print("="*80)
    
    # Query products grouped by category
    for category in categories:
        products_in_category = session.query(Product).filter_by(category_id=category.category_id).all()
        
        print(f"\n📂 {category.category_name.upper()} ({len(products_in_category)} items)")
        print("-" * 60)
        
        total_value = 0
        for product in products_in_category:
            print(f"  • {product.product_name:<25} | ${product.price:>8.2f}")
            total_value += product.price
        
        print(f"  {'Total Category Value:':<25} | ${total_value:>8.2f}")
    
    # Overall statistics
    print("\n" + "="*80)
    print("OVERALL STATISTICS")
    print("="*80)
    
    total_products = session.query(Product).count()
    total_categories = session.query(Category).count()
    average_price = session.query(Product).with_entities(Product.price).all()
    avg_price = sum(price[0] for price in average_price) / len(average_price)
    
    # Most expensive product
    most_expensive = session.query(Product).order_by(Product.price.desc()).first()
    
    # Least expensive product
    least_expensive = session.query(Product).order_by(Product.price.asc()).first()
    
    print(f"Total Products: {total_products}")
    print(f"Total Categories: {total_categories}")
    print(f"Average Product Price: ${avg_price:.2f}")
    print(f"Most Expensive: {most_expensive.product_name} (${most_expensive.price:.2f})")
    print(f"Least Expensive: {least_expensive.product_name} (${least_expensive.price:.2f})")
    
    # Products with JOIN query (showing relationship)
    print("\n" + "="*80)
    print("DETAILED PRODUCT LIST (with JOIN)")
    print("="*80)
    
    # Join query to get product with category name
    results = session.query(Product, Category).join(Category).all()
    
    print(f"{'Product Name':<25} | {'Price':<10} | {'Category'}")
    print("-" * 60)
    
    for product, category in results:
        print(f"{product.product_name:<25} | ${product.price:<9.2f} | {category.category_name}")

def main():
    """Main function to run the database forge"""
    try:
        # Create database and engine
        engine = create_database()
        
        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Check if tables are already populated
        existing_categories = session.query(Category).count()
        
        if existing_categories == 0:
            # Populate with sample data
            populate_sample_data(session)
        else:
            logger.info(f"Database already contains {existing_categories} categories. Using existing data.")
        
        # Retrieve and display data
        retrieve_and_display_data(session)
        
        # Close session
        session.close()
        logger.info("Database operations completed successfully")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()