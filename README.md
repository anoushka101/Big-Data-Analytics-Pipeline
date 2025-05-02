
# Flipkart E-commerce Data Analysis Pipeline in AWS

## Project Overview
This project implements a comprehensive big data analytics pipeline for processing and analyzing Flipkart's e-commerce dataset containing 5.7M+ records. The solution integrates multiple AWS services with PySpark for scalable data processing and real-time analytics.
![image](https://github.com/user-attachments/assets/e9c62d63-82cb-4088-95c2-f8a0bf22eb65)


## Architecture
![image](https://github.com/user-attachments/assets/d572687b-12a6-42ce-ac50-7088eb2731d6)

- **Data Ingestion**: AWS S3 Event Notifications
- **Processing**: AWS Lambda, PySpark
- **Storage**: Amazon S3
- **Monitoring**: AWS SNS for alerts
- **Visualization**: Amazon QuickSight with SPICE


## Key Features
- Automated ETL pipeline with PySpark
- Real-time data processing using Lambda triggers
- Machine learning implementation with SageMaker Autopilot
- Interactive dashboards in QuickSight
- Comprehensive data analysis including:
  - Revenue analysis by brand
  - Discount patterns by category
  - Product availability tracking
  - Seller performance metrics
  ![image](https://github.com/user-attachments/assets/d95f607f-0dbb-4b72-8f78-57ce2acb92b0)

## Technical Highlights
- **Model Performance**: 0.333 RMSE, 0.111 MSE
![image](https://github.com/user-attachments/assets/d988aa28-88a2-4d5b-9e86-3bfd035405e3)
![image](https://github.com/user-attachments/assets/81022c41-d315-424b-a051-0cd31c683170)
- **Dataset Scale**: 5.7M+ records processed
- **Key Insights**: 
  - Revenue analysis across 30+ brands
  - Category-wise discount patterns
  - Inventory optimization opportunities

