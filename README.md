# ⚡️🌍 ESG Data Pipeline with Apache Airflow

Welcome to the **ESG (Environmental, Social, and Governance)** data pipeline project!  
This repo contains an **Apache Airflow** project that reads energy statistics from **Eurostat** 🇪🇺 and uploads the processed data to **Google Cloud Platform (GCP)** ☁️📊.

---

## 📈 What does this pipeline do?

This pipeline extracts and transforms **energy-related metrics** for ESG reporting:

🔹 **Share of energy production by source**  
🔹 **Percentage of energy from renewable sources**

Using public Eurostat data, it builds clean, structured datasets and uploads them to a Google Cloud Storage bucket for further analysis or integration.

---

## 🛠️ Technologies Used

- 🌀 [Apache Airflow](https://airflow.apache.org/)
- 🌐 [Eurostat API / downloads](https://ec.europa.eu/eurostat)
- ☁️ [Google Cloud Platform (GCS)](https://cloud.google.com/storage)
- 🐍 Python 3.12
- 🐳 Docker & Docker Compose

---

## 🚀 Quick Overview

1. Dockerized Airflow setup with custom dependencies
2. Tasks (DAGs) to fetch, process, and push ESG data
3. Uses Airflow **Variables** and **Connections** for flexible configuration
4. Data is stored in **GCS buckets** for downstream use

---

