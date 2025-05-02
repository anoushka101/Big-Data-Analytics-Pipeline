
# Flipkart E-commerce Data Analysis Pipeline in AWS

## Project Overview
This project implements a comprehensive big data analytics pipeline for processing and analyzing Flipkart's e-commerce dataset containing 5.7M+ records. The solution integrates multiple AWS services with PySpark for scalable data processing and real-time analytics.

## Architecture
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

## Technical Highlights
- **Model Performance**: 0.333 RMSE, 0.111 MSE
- **Dataset Scale**: 5.7M+ records processed
- **Key Insights**: 
  - Revenue analysis across 30+ brands
  - Category-wise discount patterns
  - Inventory optimization opportunities

