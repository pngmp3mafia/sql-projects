-- * E-COMMERCE USER BEHAVIOR & PRODUCT PERFORMANCE

-- This script demonstrates comprehensive analytics for an e-commerce platform
-- using advanced SQL techniques including:
-- Common Table Expressions (CTEs)
-- Window functions
-- Hierarchical queries
-- Pivoting data
-- Advanced aggregations and statistical analysis
-- JSON handling
-- Temporal queries

-- SECTION 0: Database Creation and Schema Setup
-- Create database (SQL Server syntax)
CREATE DATABASE ECommerceAnalytics;
GO
USE ECommerceAnalytics;
GO

-- Create tables with appropriate data types, constraints, and relationships
CREATE TABLE categories (
    id INT PRIMARY KEY IDENTITY(1,1),
    name VARCHAR(100) NOT NULL,
    parent_id INT NULL,
    description TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_category_parent FOREIGN KEY (parent_id) REFERENCES categories(id)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY IDENTITY(1,1),
    product_name VARCHAR(255) NOT NULL,
    category_id INT NOT NULL,
    description TEXT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    inventory_count INT NOT NULL DEFAULT 0,
    reorder_threshold INT NOT NULL DEFAULT 10,
    rating DECIMAL(3,2) NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES categories(id),
    CONSTRAINT chk_product_price CHECK (price >= 0),
    CONSTRAINT chk_product_cost CHECK (cost >= 0),
    CONSTRAINT chk_product_inventory CHECK (inventory_count >= 0)
);

CREATE INDEX idx_product_category ON products(category_id);
CREATE INDEX idx_product_name ON products(product_name);
CREATE INDEX idx_product_price ON products(price);

CREATE TABLE users (
    user_id INT PRIMARY KEY IDENTITY(1,1),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NULL,
    last_name VARCHAR(100) NULL,
    phone VARCHAR(20) NULL,
    date_of_birth DATE NULL,
    address_line1 VARCHAR(255) NULL,
    address_line2 VARCHAR(255) NULL,
    city VARCHAR(100) NULL,
    state VARCHAR(100) NULL,
    postal_code VARCHAR(20) NULL,
    country VARCHAR(100) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login_date DATETIME NULL,
    last_activity_date DATETIME NULL,
    is_active BIT NOT NULL DEFAULT 1,
    lifetime_value DECIMAL(12,2) NOT NULL DEFAULT 0,
    CONSTRAINT chk_user_lifetime_value CHECK (lifetime_value >= 0)
);

CREATE INDEX idx_user_email ON users(email);
CREATE INDEX idx_user_last_activity ON users(last_activity_date);

CREATE TABLE orders (
    order_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT NOT NULL,
    order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    shipping_address_line1 VARCHAR(255) NOT NULL,
    shipping_address_line2 VARCHAR(255) NULL,
    shipping_city VARCHAR(100) NOT NULL,
    shipping_state VARCHAR(100) NOT NULL,
    shipping_postal_code VARCHAR(20) NOT NULL,
    shipping_country VARCHAR(100) NOT NULL,
    billing_address_line1 VARCHAR(255) NOT NULL,
    billing_address_line2 VARCHAR(255) NULL,
    billing_city VARCHAR(100) NOT NULL,
    billing_state VARCHAR(100) NOT NULL,
    billing_postal_code VARCHAR(20) NOT NULL,
    billing_country VARCHAR(100) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    shipping_method VARCHAR(50) NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL,
    tax DECIMAL(10,2) NOT NULL,
    shipping_cost DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    is_new_customer BIT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order_user FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT chk_order_subtotal CHECK (subtotal >= 0),
    CONSTRAINT chk_order_tax CHECK (tax >= 0),
    CONSTRAINT chk_order_shipping CHECK (shipping_cost >= 0),
    CONSTRAINT chk_order_total CHECK (total_amount >= 0)
);

CREATE INDEX idx_order_user ON orders(user_id);
CREATE INDEX idx_order_date ON orders(order_date);
CREATE INDEX idx_order_status ON orders(status);

CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY IDENTITY(1,1),
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2) NOT NULL DEFAULT 0,
    tax_amount DECIMAL(10,2) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orderitem_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_orderitem_product FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT chk_orderitem_quantity CHECK (quantity > 0),
    CONSTRAINT chk_orderitem_price CHECK (unit_price >= 0),
    CONSTRAINT chk_orderitem_discount CHECK (discount >= 0),
    CONSTRAINT chk_orderitem_tax CHECK (tax_amount >= 0)
);

CREATE INDEX idx_orderitem_order ON order_items(order_id);
CREATE INDEX idx_orderitem_product ON order_items(product_id);

CREATE TABLE user_event_log (
    event_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT NULL, -- Can be null for anonymous users
    session_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    page_url VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_address VARCHAR(50) NULL,
    user_agent TEXT NULL,
    event_data NVARCHAR(MAX) NULL, -- JSON data
    CONSTRAINT fk_event_user FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE INDEX idx_event_user ON user_event_log(user_id);
CREATE INDEX idx_event_type ON user_event_log(event_type);
CREATE INDEX idx_event_date ON user_event_log(created_at);

-- Create an optimized view for common analytics queries
CREATE VIEW vw_order_details AS
SELECT 
    o.order_id,
    o.user_id,
    u.email,
    o.order_date,
    o.status,
    o.total_amount,
    o.is_new_customer,
    oi.product_id,
    p.product_name,
    p.category_id,
    c.name AS category_name,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_total,
    p.cost,
    (oi.quantity * oi.unit_price) - (oi.quantity * p.cost) AS profit
FROM orders o
JOIN users u ON o.user_id = u.user_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.id;

-- Insert sample data for testing (abbreviated version)
INSERT INTO categories (name, parent_id, description)
VALUES 
    ('Electronics', NULL, 'Electronic devices and accessories'),
    ('Computers', 1, 'Desktop and laptop computers'),
    ('Smartphones', 1, 'Mobile phones and accessories'),
    ('Clothing', NULL, 'Apparel and fashion items'),
    ('Men''s Clothing', 4, 'Clothing for men'),
    ('Women''s Clothing', 4, 'Clothing for women');

-- Insert sample products
INSERT INTO products (product_name, category_id, description, sku, price, cost, inventory_count)
VALUES
    ('Laptop Pro X1', 2, 'High-performance laptop with 16GB RAM', 'LAP-X1-001', 1299.99, 899.99, 50),
    ('Smartphone Galaxy S', 3, 'Latest smartphone with 128GB storage', 'PHN-GS-001', 899.99, 599.99, 100),
    ('Men''s Classic T-Shirt', 5, 'Cotton t-shirt in various colors', 'MTS-CL-001', 24.99, 8.50, 200),
    ('Women''s Denim Jeans', 6, 'Stylish denim jeans for women', 'WDJ-DN-001', 49.99, 15.75, 150);

-- Sample function to generate test data
CREATE OR ALTER PROCEDURE GenerateTestData
AS
BEGIN
    -- Implementation would go here
    -- For brevity, just declaring the procedure
    PRINT 'This procedure would populate all tables with realistic test data';
END;
-- Create cohorts based on user signup dates and analyze retention
WITH user_cohorts AS (
    SELECT 
        DATE_TRUNC('month', created_at) AS cohort_month,
        user_id,
        created_at AS signup_date
    FROM users
    WHERE created_at >= CURRENT_DATE - INTERVAL '1 year'
),

user_monthly_activity AS (
    SELECT 
        u.user_id,
        u.cohort_month,
        DATE_TRUNC('month', o.created_at) AS activity_month,
        COUNT(DISTINCT o.order_id) AS num_orders,
        SUM(o.total_amount) AS total_spent
    FROM user_cohorts u
    LEFT JOIN orders o ON u.user_id = o.user_id
    WHERE o.created_at IS NULL OR o.created_at >= u.signup_date
    GROUP BY u.user_id, u.cohort_month, DATE_TRUNC('month', o.created_at)
),

cohort_retention AS (
    SELECT 
        cohort_month,
        activity_month,
        DATEDIFF('month', cohort_month, activity_month) AS months_since_signup,
        COUNT(DISTINCT user_id) AS active_users,
        SUM(num_orders) AS total_orders,
        SUM(total_spent) AS total_revenue
    FROM user_monthly_activity
    GROUP BY cohort_month, activity_month
),

cohort_size AS (
    SELECT 
        cohort_month, 
        COUNT(DISTINCT user_id) AS num_users
    FROM user_cohorts
    GROUP BY cohort_month
)

SELECT 
    cr.cohort_month,
    cs.num_users AS cohort_size,
    cr.months_since_signup,
    cr.active_users,
    ROUND(100.0 * cr.active_users / cs.num_users, 2) AS retention_rate,
    cr.total_orders,
    cr.total_revenue,
    ROUND(cr.total_revenue / cr.active_users, 2) AS revenue_per_active_user
FROM cohort_retention cr
JOIN cohort_size cs ON cr.cohort_month = cs.cohort_month
WHERE cr.months_since_signup >= 0
ORDER BY cr.cohort_month, cr.months_since_signup;

-- SECTION 2: Product Performance Analysis with Statistical Metrics
-- Analyze product performance with advanced metrics including percentiles and moving averages
WITH product_daily_stats AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category_id,
        c.category_name,
        DATE_TRUNC('day', oi.created_at) AS sale_date,
        COUNT(oi.order_item_id) AS units_sold,
        SUM(oi.quantity * oi.unit_price) AS revenue,
        SUM(oi.quantity * oi.unit_price) - SUM(oi.quantity * p.cost) AS profit
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE oi.created_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY p.product_id, p.product_name, p.category_id, c.category_name, DATE_TRUNC('day', oi.created_at)
),

product_stats_with_windows AS (
    SELECT 
        product_id,
        product_name,
        category_id,
        category_name,
        sale_date,
        units_sold,
        revenue,
        profit,
        -- Moving averages
        AVG(units_sold) OVER (PARTITION BY product_id ORDER BY sale_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_sold_7day_ma,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7day_ma,
        
        -- Cumulative metrics
        SUM(units_sold) OVER (PARTITION BY product_id ORDER BY sale_date) AS cumulative_units,
        SUM(revenue) OVER (PARTITION BY product_id ORDER BY sale_date) AS cumulative_revenue,
        
        -- Rankings
        RANK() OVER (PARTITION BY category_id, sale_date ORDER BY revenue DESC) AS category_revenue_rank,
        PERCENT_RANK() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS category_percentile
    FROM product_daily_stats
),

category_stats AS (
    SELECT 
        category_id,
        category_name,
        COUNT(DISTINCT product_id) AS num_products,
        SUM(units_sold) AS total_units_sold,
        SUM(revenue) AS total_revenue,
        SUM(profit) AS total_profit,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue) AS median_revenue,
        STDDEV(revenue) AS revenue_stddev,
        AVG(revenue) AS avg_revenue
    FROM product_daily_stats
    GROUP BY category_id, category_name
)

SELECT 
    ps.product_id,
    ps.product_name,
    ps.category_name,
    SUM(ps.units_sold) AS total_units_90days,
    SUM(ps.revenue) AS total_revenue_90days,
    SUM(ps.profit) AS total_profit_90days,
    ROUND(SUM(ps.profit) / SUM(ps.revenue) * 100, 2) AS profit_margin,
    AVG(ps.units_sold_7day_ma) AS avg_7day_moving_units,
    COUNT(DISTINCT ps.sale_date) AS days_with_sales,
    MIN(ps.category_revenue_rank) AS best_category_rank,
    cs.median_revenue AS category_median_revenue,
    cs.avg_revenue AS category_avg_revenue,
    CASE 
        WHEN SUM(ps.revenue) > cs.avg_revenue + cs.revenue_stddev THEN 'Top Performer'
        WHEN SUM(ps.revenue) < cs.avg_revenue - cs.revenue_stddev THEN 'Underperforming'
        ELSE 'Average'
    END AS performance_category
FROM product_stats_with_windows ps
JOIN category_stats cs ON ps.category_id = cs.category_id
GROUP BY 
    ps.product_id, 
    ps.product_name, 
    ps.category_name,
    cs.median_revenue,
    cs.avg_revenue,
    cs.revenue_stddev
ORDER BY total_profit_90days DESC;

-- SECTION 3: Customer Segmentation with RFM Analysis
-- Perform Recency-Frequency-Monetary analysis to segment customers
WITH customer_rfm AS (
    SELECT 
        user_id,
        DATEDIFF('day', MAX(created_at), CURRENT_DATE) AS recency,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(total_amount) AS monetary
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY user_id
),

rfm_scores AS (
    SELECT 
        user_id,
        recency,
        frequency, 
        monetary,
        NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency) AS f_score,
        NTILE(5) OVER (ORDER BY monetary) AS m_score
    FROM customer_rfm
),

rfm_final AS (
    SELECT 
        user_id,
        recency,
        frequency, 
        monetary,
        r_score,
        f_score,
        m_score,
        r_score * 100 + f_score * 10 + m_score AS rfm_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 3 AND f_score >= 1 AND m_score >= 2 THEN 'Potential Loyalists'
            WHEN r_score >= 4 AND f_score <= 2 AND m_score <= 2 THEN 'New Customers'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Hibernating'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cannot Lose Them'
            ELSE 'Others'
        END AS segment
    FROM rfm_scores
)

SELECT 
    segment,
    COUNT(*) AS num_customers,
    ROUND(AVG(recency), 1) AS avg_recency_days,
    ROUND(AVG(frequency), 1) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary,
    ROUND(SUM(monetary), 2) AS total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers,
    ROUND(100.0 * SUM(monetary) / SUM(SUM(monetary)) OVER (), 2) AS pct_of_revenue
FROM rfm_final
GROUP BY segment
ORDER BY avg_monetary DESC;

-- SECTION 4: Market Basket Analysis with Self-Joining
-- Find frequently co-purchased products
WITH order_product_pairs AS (
    SELECT 
        o1.order_id,
        o1.product_id AS product_1,
        o2.product_id AS product_2
    FROM order_items o1
    JOIN order_items o2 ON 
        o1.order_id = o2.order_id AND 
        o1.product_id < o2.product_id  -- Ensure unique pairs
    WHERE o1.created_at >= CURRENT_DATE - INTERVAL '90 days'
),

product_pair_counts AS (
    SELECT 
        product_1,
        product_2,
        COUNT(*) AS times_purchased_together
    FROM order_product_pairs
    GROUP BY product_1, product_2
),

product_counts AS (
    SELECT 
        product_id,
        COUNT(DISTINCT order_id) AS times_purchased
    FROM order_items
    WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY product_id
)

SELECT 
    p1.product_name AS product_1_name,
    p2.product_name AS product_2_name,
    pc.times_purchased_together,
    c1.times_purchased AS product_1_purchases,
    c2.times_purchased AS product_2_purchases,
    ROUND(100.0 * pc.times_purchased_together / c1.times_purchased, 2) AS pct_of_product1_orders,
    ROUND(100.0 * pc.times_purchased_together / c2.times_purchased, 2) AS pct_of_product2_orders,
    -- Lift is a measure of how much more likely products are purchased together versus by chance
    ROUND(
        (1.0 * pc.times_purchased_together * (SELECT COUNT(DISTINCT order_id) FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '90 days')) /
        (1.0 * c1.times_purchased * c2.times_purchased),
        4
    ) AS lift
FROM product_pair_counts pc
JOIN product_counts c1 ON pc.product_1 = c1.product_id
JOIN product_counts c2 ON pc.product_2 = c2.product_id
JOIN products p1 ON pc.product_1 = p1.product_id
JOIN products p2 ON pc.product_2 = p2.product_id
WHERE pc.times_purchased_together >= 10  -- Minimum support threshold
ORDER BY lift DESC, times_purchased_together DESC
LIMIT 20;

-- SECTION 5: Hierarchical Category Analysis with Recursive CTE
-- Analyze product hierarchy and roll up metrics through the category tree
WITH RECURSIVE category_hierarchy AS (
    -- Base case: top-level categories
    SELECT 
        id,
        name,
        parent_id,
        1 AS level,
        CAST(id AS VARCHAR(255)) AS path
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive case: categories with parents
    SELECT 
        c.id,
        c.name,
        c.parent_id,
        ch.level + 1,
        ch.path || ',' || c.id
    FROM categories c
    JOIN category_hierarchy ch ON c.parent_id = ch.id
),

category_sales AS (
    SELECT 
        p.category_id,
        DATE_TRUNC('month', o.created_at) AS month,
        SUM(oi.quantity * oi.unit_price) AS revenue,
        SUM(oi.quantity) AS units_sold
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY p.category_id, DATE_TRUNC('month', o.created_at)
)

SELECT 
    ch.path,
    ch.level,
    REPEAT('    ', ch.level - 1) || ch.name AS category_name,
    COUNT(DISTINCT p.product_id) AS num_products,
    COALESCE(SUM(cs.revenue), 0) AS total_revenue,
    COALESCE(SUM(cs.units_sold), 0) AS total_units
FROM category_hierarchy ch
LEFT JOIN products p ON ch.id = p.category_id
LEFT JOIN category_sales cs ON p.category_id = cs.category_id
GROUP BY ch.id, ch.name, ch.level, ch.path
ORDER BY ch.path;

-- SECTION 6: Customer Journey Analysis with JSON and Temporal Logic
-- Analyze customer touchpoints and conversion paths using JSON functions and event data
WITH user_events AS (
    SELECT 
        user_id,
        event_type,
        created_at,
        page_url,
        JSON_EXTRACT(event_data, '$.source') AS traffic_source,
        JSON_EXTRACT(event_data, '$.device_type') AS device_type,
        JSON_EXTRACT(event_data, '$.product_id') AS product_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) AS event_sequence
    FROM user_event_log
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
),

conversion_paths AS (
    SELECT 
        user_id,
        STRING_AGG(event_type, ' > ' ORDER BY created_at) AS event_path,
        COUNT(*) AS path_length,
        MIN(created_at) AS first_touch,
        MAX(created_at) AS last_touch,
        DATEDIFF('minute', MIN(created_at), MAX(created_at)) AS journey_minutes,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS converted
    FROM user_events
    GROUP BY user_id
    HAVING COUNT(*) >= 2  -- At least two touchpoints
),

attribution_model AS (
    SELECT 
        ue.user_id,
        ue.traffic_source AS first_touch_source,
        LAST_VALUE(ue.traffic_source) OVER (
            PARTITION BY ue.user_id 
            ORDER BY ue.created_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_source,
        MAX(CASE WHEN o.total_amount IS NOT NULL THEN o.total_amount ELSE 0 END) AS conversion_value
    FROM user_events ue
    LEFT JOIN orders o ON 
        ue.user_id = o.user_id AND 
        ue.event_type = 'purchase' AND
        o.created_at BETWEEN ue.created_at AND ue.created_at + INTERVAL '10 minutes'
    GROUP BY ue.user_id
)

SELECT 
    cp.event_path,
    COUNT(*) AS path_count,
    ROUND(AVG(cp.journey_minutes), 2) AS avg_journey_minutes,
    SUM(cp.converted) AS conversions,
    ROUND(100.0 * SUM(cp.converted) / COUNT(*), 2) AS conversion_rate,
    STRING_AGG(DISTINCT am.first_touch_source, ', ') AS common_first_sources,
    STRING_AGG(DISTINCT am.last_touch_source, ', ') AS common_last_sources,
    SUM(am.conversion_value) AS total_revenue,
    ROUND(SUM(am.conversion_value) / SUM(cp.converted), 2) AS avg_order_value
FROM conversion_paths cp
JOIN attribution_model am ON cp.user_id = am.user_id
GROUP BY cp.event_path
HAVING COUNT(*) >= 5  -- Only common paths
ORDER BY path_count DESC
LIMIT 20;

-- SECTION 7: Anomaly Detection with Statistical Methods
-- Identify unusual patterns in daily sales data
WITH daily_sales AS (
    SELECT 
        DATE_TRUNC('day', created_at) AS sale_date,
        COUNT(order_id) AS num_orders,
        SUM(total_amount) AS daily_revenue,
        COUNT(DISTINCT user_id) AS unique_customers
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', created_at)
),

sales_stats AS (
    SELECT 
        sale_date,
        num_orders,
        daily_revenue,
        unique_customers,
        -- Rolling statistics for anomaly detection (30-day window)
        AVG(num_orders) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_orders_30d,
        
        STDDEV(num_orders) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS stddev_orders_30d,
        
        AVG(daily_revenue) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_revenue_30d,
        
        STDDEV(daily_revenue) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS stddev_revenue_30d
    FROM daily_sales
)

SELECT 
    sale_date,
    num_orders,
    daily_revenue,
    unique_customers,
    avg_orders_30d,
    stddev_orders_30d,
    avg_revenue_30d,
    stddev_revenue_30d,
    -- Z-scores to measure deviation from typical patterns
    ROUND((num_orders - avg_orders_30d) / NULLIF(stddev_orders_30d, 0), 2) AS orders_z_score,
    ROUND((daily_revenue - avg_revenue_30d) / NULLIF(stddev_revenue_30d, 0), 2) AS revenue_z_score,
    -- Flag anomalies (Z-score > 2 or < -2 indicates unusual activity)
    CASE 
        WHEN ABS((num_orders - avg_orders_30d) / NULLIF(stddev_orders_30d, 0)) > 2 
             OR ABS((daily_revenue - avg_revenue_30d) / NULLIF(stddev_revenue_30d, 0)) > 2
        THEN 1 
        ELSE 0 
    END AS is_anomaly
FROM sales_stats
WHERE sale_date > CURRENT_DATE - INTERVAL '60 days' -- Allow 30 days to establish baseline
ORDER BY is_anomaly DESC, sale_date DESC;

-- SECTION 8: Dynamic Pivot Table Creation with Advanced Aggregation
-- Create a dynamic pivot table showing product category performance by month
-- First, get the distinct months in our time range
WITH months AS (
    SELECT DISTINCT DATE_TRUNC('month', created_at) AS month
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
    ORDER BY month
),

-- Get the category sales data
category_monthly_sales AS (
    SELECT 
        c.category_name,
        DATE_TRUNC('month', o.created_at) AS month,
        SUM(oi.quantity * oi.unit_price) AS revenue
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY c.category_name, DATE_TRUNC('month', o.created_at)
)

-- Dynamically construct and execute a pivot query
-- Note: This is implemented differently based on specific database systems
-- This example uses a PostgreSQL-style approach with string aggregation
SELECT
    category_name,
    -- For each category, calculate total, growth rate, and percentage of overall
    SUM(revenue) AS annual_revenue,
    ROUND(100.0 * (
        SUM(CASE WHEN month >= CURRENT_DATE - INTERVAL '6 months' THEN revenue ELSE 0 END) /
        NULLIF(SUM(CASE WHEN month < CURRENT_DATE - INTERVAL '6 months' THEN revenue ELSE 0 END), 0) - 1
    ), 2) AS six_month_growth_pct,
    -- Create pivot for each month
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 1 THEN revenue ELSE 0 END) AS "Jan",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 2 THEN revenue ELSE 0 END) AS "Feb",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 3 THEN revenue ELSE 0 END) AS "Mar",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 4 THEN revenue ELSE 0 END) AS "Apr",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 5 THEN revenue ELSE 0 END) AS "May",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 6 THEN revenue ELSE 0 END) AS "Jun",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 7 THEN revenue ELSE 0 END) AS "Jul",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 8 THEN revenue ELSE 0 END) AS "Aug",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 9 THEN revenue ELSE 0 END) AS "Sep",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 10 THEN revenue ELSE 0 END) AS "Oct",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 11 THEN revenue ELSE 0 END) AS "Nov",
    MAX(CASE WHEN EXTRACT(MONTH FROM month) = 12 THEN revenue ELSE 0 END) AS "Dec"
FROM category_monthly_sales
GROUP BY category_name
ORDER BY annual_revenue DESC;

-- SECTION 9: Optimized Query for Executive Dashboard
-- Create a single comprehensive query that provides key business metrics for an executive dashboard
WITH revenue_trends AS (
    SELECT 
        DATE_TRUNC('day', created_at) AS date,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT user_id) AS customers,
        SUM(total_amount) AS revenue,
        SUM(CASE WHEN is_new_customer = TRUE THEN total_amount ELSE 0 END) AS new_customer_revenue,
        100.0 * SUM(CASE WHEN is_new_customer = TRUE THEN total_amount ELSE 0 END) / 
            NULLIF(SUM(total_amount), 0) AS new_customer_revenue_pct
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', created_at)
),

product_performance AS (
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.quantity * oi.unit_price) AS revenue,
        SUM(oi.quantity * oi.unit_price) - SUM(oi.quantity * p.cost) AS profit,
        ROUND(100.0 * (SUM(oi.quantity * oi.unit_price) - SUM(oi.quantity * p.cost)) / 
            NULLIF(SUM(oi.quantity * oi.unit_price), 0), 2) AS profit_margin
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY p.product_id, p.product_name, c.category_name
),

user_metrics AS (
    SELECT 
        COUNT(DISTINCT user_id) AS total_active_users,
        COUNT(DISTINCT CASE WHEN created_at >= CURRENT_DATE - INTERVAL '30 days' THEN user_id END) AS new_users_30d,
        ROUND(AVG(lifetime_value), 2) AS avg_customer_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) AS median_customer_value
    FROM users
    WHERE last_activity_date >= CURRENT_DATE - INTERVAL '90 days'
),

conversion_funnel AS (
    SELECT 
        'Product Views' AS funnel_stage,
        COUNT(*) AS events,
        100 AS pct_previous,
        COUNT(*) / (SELECT COUNT(DISTINCT user_id) FROM user_event_log 
                   WHERE event_type = 'product_view' 
                   AND created_at >= CURRENT_DATE - INTERVAL '30 days') AS conversion_rate
    FROM user_event_log
    WHERE event_type = 'product_view' 
    AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    
    UNION ALL
    
    SELECT 
        'Add to Cart' AS funnel_stage,
        COUNT(*) AS events,
        100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM user_event_log 
                                 WHERE event_type = 'product_view' 
                                 AND created_at >= CURRENT_DATE - INTERVAL '30 days'), 0) AS pct_previous,
        COUNT(*) / (SELECT COUNT(DISTINCT user_id) FROM user_event_log 
                   WHERE event_type = 'add_to_cart' 
                   AND created_at >= CURRENT_DATE - INTERVAL '30 days') AS conversion_rate
    FROM user_event_log
    WHERE event_type = 'add_to_cart' 
    AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    
    UNION ALL
    
    SELECT 
        'Checkout Started' AS funnel_stage,
        COUNT(*) AS events,
        100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM user_event_log 
                                 WHERE event_type = 'add_to_cart' 
                                 AND created_at >= CURRENT_DATE - INTERVAL '30 days'), 0) AS pct_previous,
        COUNT(*) / (SELECT COUNT(DISTINCT user_id) FROM user_event_log 
                   WHERE event_type = 'checkout_start' 
                   AND created_at >= CURRENT_DATE - INTERVAL '30 days') AS conversion_rate
    FROM user_event_log
    WHERE event_type = 'checkout_start' 
    AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    
    UNION ALL
    
    SELECT 
        'Purchase Completed' AS funnel_stage,
        COUNT(*) AS events,
        100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM user_event_log 
                                 WHERE event_type = 'checkout_start' 
                                 AND created_at >= CURRENT_DATE - INTERVAL '30 days'), 0) AS pct_previous,
        COUNT(*) / (SELECT COUNT(DISTINCT user_id) FROM user_event_log 
                   WHERE event_type = 'purchase' 
                   AND created_at >= CURRENT_DATE - INTERVAL '30 days') AS conversion_rate
    FROM user_event_log
    WHERE event_type = 'purchase' 
    AND created_at >= CURRENT_DATE - INTERVAL '30 days'
)

-- Final combined dashboard query
SELECT 
    'Period' AS metric_name, 
    CONCAT(MIN(rt.date), ' to ', MAX(rt.date)) AS metric_value 
FROM revenue_trends rt

UNION ALL SELECT 'Total Revenue (30d)', TO_CHAR(SUM(revenue), 'FM$999,999,999.00') FROM revenue_trends
UNION ALL SELECT 'Total Orders (30d)', TO_CHAR(SUM(orders), 'FM999,999') FROM revenue_trends
UNION ALL SELECT 'Unique Customers (30d)', TO_CHAR(SUM(customers), 'FM999,999') FROM revenue_trends
UNION ALL SELECT 'Average Order Value', TO_CHAR(SUM(revenue) / NULLIF(SUM(orders), 0), 'FM$999,999.00') FROM revenue_trends
UNION ALL SELECT 'New Customer Revenue %', TO_CHAR(100.0 * SUM(new_customer_revenue) / NULLIF(SUM(revenue), 0), 'FM990.00%') FROM revenue_trends

UNION ALL SELECT 'Total Active Users', TO_CHAR(total_active_users, 'FM999,999') FROM user_metrics
UNION ALL SELECT 'New Users (30d)', TO_CHAR(new_users_30d, 'FM