## Project Name: Real Time Stock Market Analysis

 The project implements a real-time data pipeline that extracts stock data from vantage API, streams it through Apache Kafka, processes it with Apache Spark, and loads it into a postgres database. 
 
 All components are containerized with Docker for easy deployment.

 ### Data Pipeline Architecture
 ![Data Pipeline Architecture](./_img/real_time_pipeline.png)

 
Project Tech Stack and Flow
  - `Kafka UI → inspect topics/messages.`
  - `API → produces JSON events into Kafka.`
  - `Spark → consumes from Kafka, writes to Postgres.`
  - `Postgres → stores results for analytics.`
  - `pgAdmin → manage Postgres visually.`
  - `Power BI → external (connects to Postgres database).`