-- Top countries by combined engagement score
SELECT 
    Country,
    SUM(Users_productsSold) +
    SUM(Buyers_Total) +
    SUM(Sellers_Total) AS engagement_score
FROM ecom_gold_table
GROUP BY Country
ORDER BY engagement_score DESC;


-- Buyer to Seller ratio (market health)
-- Insight: market gap
SELECT 
    Country,
    SUM(Buyers_Total) AS buyers,
    SUM(Sellers_Total) AS sellers,
    CASE 
        WHEN SUM(Sellers_Total) = 0 THEN 0
        ELSE SUM(Buyers_Total) / SUM(Sellers_Total)
    END AS buyer_seller_ratio
FROM ecom_gold_table
GROUP BY Country
ORDER BY buyer_seller_ratio DESC;


-- Top 5 countries by user growth potential
-- Insight: young + active users
SELECT 
    Country,
    AVG(Users_account_age_years) AS avg_age,
    AVG(Users_productsWished) AS wishlist_activity
FROM ecom_gold_table
GROUP BY Country
ORDER BY wishlist_activity DESC
LIMIT 5;


-- Gender imbalance in buyers
-- Gender imbalance in buyers
SELECT 
    Country,
    SUM(Buyers_Female) AS female_buyers,
    SUM(Buyers_Male) AS male_buyers,
    ABS(SUM(Buyers_Female) - SUM(Buyers_Male)) AS gap
FROM ecom_gold_table
GROUP BY Country
ORDER BY gap DESC;


-- Correlation-like analysis (buyers vs sellers)
SELECT 
    Country,
    SUM(Buyers_Total) AS buyers,
    SUM(Sellers_Total) AS sellers,
    (SUM(Buyers_Total) * 1.0 / NULLIF(SUM(Sellers_Total), 0)) AS ratio
FROM ecom_gold_table
GROUP BY Country;


-- Active vs passive users
SELECT 
    Country,
    SUM(CASE WHEN Users_productsSold > 0 THEN 1 ELSE 0 END) AS active_users,
    COUNT(*) AS total_users
FROM ecom_gold_table
GROUP BY Country;