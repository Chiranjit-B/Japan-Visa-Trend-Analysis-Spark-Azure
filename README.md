# Japan-Visa-Trend-Analysis-Spark-Azure
![image](https://github.com/user-attachments/assets/d9f2c84f-e307-482a-a252-e24e775ac44c)


# 🇯🇵 Japan Visa Data Analytics Using Apache Spark

This project focuses on large-scale data processing and visualization using Apache Spark and Docker. We analyze Japanese visa issuance data across multiple years and countries to uncover insights about trends, regional growth patterns, and more.

---

## 📂 Project Structure

```
SparkClusters/
│
├── src/
│   ├── visualisation.py               # Main PySpark script for data processing and visualization
│   ├── visa_number_in_japan.csv        # Input dataset containing visa data
│   ├── requirements.txt                # Required Python dependencies for visualization
│   └── Dockerfile.spark                # Dockerfile used to build custom Spark image
│
├── spark-clusters_key.pem             # SSH private key for accessing the Azure VM
├── docker-compose.yml                 # Compose file to launch Spark Master and Workers
├── upload_files.sh                    # Script to upload project files to Azure VM
├── download_files.sh                  # Script to download output files and logs from VM
├── run_and_log.sh                     # Shell script to execute the Spark job and save logs
└── README.md                          # This documentation file
```

---

## 💡 Project Overview

The goal of this project is to perform end-to-end big data processing and visual analytics using Apache Spark deployed inside Docker containers on a remote Azure Virtual Machine (VM). The process includes:

1. **Uploading the dataset and code** from a local machine to the Azure VM.
2. **Running a Spark cluster** using Docker Compose to simulate distributed processing.
3. **Cleaning and transforming the dataset** using PySpark.
4. **Generating multiple insightful visualizations** using Plotly.
5. **Saving results in HTML format** and downloading them back to the local machine.
6. **Logging the Spark job execution** for monitoring and debugging.

---

## ⚙️ How It Works (Step-by-Step)

### 1. Uploading Files
Use `upload_files.sh` to securely upload source code, input CSV, Docker files, and the requirements list to the VM:
```bash
scp -i spark-clusters_key.pem -r ./src/* azureuser@<vm_ip>:/home/azureuser/spark-cluster-upload
```

### 2. Setting Up Spark Cluster on Azure
Navigate to the project directory on your Azure VM and bring up the cluster:
```bash
cd ~/spark-cluster-upload
sudo docker compose up -d --build
```
This launches one Spark master and four worker containers defined in `docker-compose.yml`, all using the image built with `Dockerfile.spark`.

### 3. Running Spark Job
To execute the PySpark job (`visualisation.py`) inside the Spark cluster and log the output:
```bash
./run_and_log.sh
```
The job will process the visa data and generate HTML visualizations under `/home/azureuser/output`, while logs are saved in `/home/azureuser/logs`.

### 4. Downloading Results
Use `download_files.sh` to bring all generated HTML files and logs back to your local machine:
```bash
scp -i spark-clusters_key.pem /home/azureuser/spark-cluster-upload/output/*.html ./src/output/
scp -i spark-clusters_key.pem /home/azureuser/logs/*.txt ./src/output/
```

---

## 📊 Visualizations
The following analyses are performed in `visualisation.py` using PySpark and Plotly:

1. **Line Chart** - Yearly visa issuance trends across continents.
2. **Bar Chart** - Top 10 countries with most visas issued in 2017.
3. **Choropleth Map** - Animated map showing visa distribution by country from 2006 to 2017.
4. **Line Chart** - Year-over-year growth rate of visas per continent.
5. **Bar Chart with Labels** - Peak visa issuance year for each continent.

All charts are saved as HTML files in the output directory and can be opened in any browser.

---

## 📁 Detailed File Descriptions

- **`visualisation.py`**: Core processing script that:
  - Loads and cleans visa CSV data
  - Corrects country names using fuzzy matching
  - Maps countries to continents using `pycountry` and `pycountry_convert`
  - Aggregates visa numbers by year, country, and continent
  - Uses Spark SQL queries for grouping, filtering, and ranking
  - Converts PySpark DataFrames to Pandas for visualization
  - Generates five different visualizations using Plotly and writes them as `.html` files
  - Saves a cleaned version of the data to a CSV file in the output directory

- **`Dockerfile.spark`**: Defines a custom Spark image based on Bitnami Spark, installs Python3 and required packages using `pip3 install -r requirements.txt`.

- **`docker-compose.yml`**: Orchestrates container deployment for the Spark Master and 4 Spark Workers, exposing necessary ports and linking nodes.

- **`requirements.txt`**: Specifies all required Python packages: `plotly`, `pycountry`, `pycountry-convert`, and `fuzzywuzzy`.

- **`run_and_log.sh`**: A shell script that:
  - Creates a `logs` folder (if it doesn't exist)
  - Submits the Spark job using `spark-submit` on one of the worker containers
  - Redirects the logs with a timestamp to the `logs` folder for auditing or debugging

- **`upload_files.sh`**: Uses `scp` to recursively upload the `src/` folder (dataset + code + config) to Azure VM's `spark-cluster-upload` directory.

- **`download_files.sh`**: Uses `scp` to recursively download all `.html` visualizations and `.txt` log files from VM back to your local machine.

- **`spark-clusters_key.pem`**: SSH private key file used for authentication with the remote VM.

---

## 🔥 Technologies Used
- **Apache Spark**: For distributed big data processing
- **Docker + Docker Compose**: For containerized cluster orchestration
- **Plotly**: For interactive visualizations
- **Azure VM**: For remote infrastructure
- **PySpark**: Python API for Apache Spark

---

## ✅ Outcomes
This project demonstrates:
- Data cleaning, transformation, and enrichment at scale
- Country-to-continent mapping using fuzzy logic
- Complex multi-visual analytics pipeline using Spark and Plotly
- End-to-end automation with Docker and SSH
- Logging and traceability of analytics jobs

---

## 🧠 Inner Workings of the Project (How It Runs Internally)

1. **Data Ingestion**:
   - The raw CSV file `visa_number_in_japan.csv` is read using PySpark's `read.csv()` method.
   - Column names are standardized by removing whitespace and special characters.

2. **Data Cleaning**:
   - Null columns are dropped.
   - Invalid or misspelled country names are corrected using a fuzzy matching function (`fuzzywuzzy.process.extractOne`).
   - Manual overrides are used for names not correctly matched by fuzzy logic.

3. **Enrichment**:
   - Each country is mapped to a continent using `pycountry_convert` to enhance geographic analytics.
   - A new column `continent` is added to the DataFrame.

4. **Spark SQL Processing**:
   - The cleaned and enriched data is registered as a temporary global SQL table (`global_temp.japan_visa`).
   - SQL queries are used to group data by year, country, and continent.
   - Year-over-year calculations and ranking (peak year per continent) are performed using Spark window functions (`lag`, `row_number`).

5. **Visualization**:
   - For each visual, the Spark DataFrame is converted to Pandas using `.toPandas()`.
   - Visuals include line plots, choropleth maps, and grouped bar charts using Plotly Express.
   - These are saved as `.html` files in the `output/` directory for easy viewing in any browser.

6. **Execution & Logging**:
   - `run_and_log.sh` runs the Spark job and stores logs with a timestamp inside `logs/`.
   - This log file captures runtime Spark details, warnings, and errors for transparency and troubleshooting.

7. **Output Handling**:
   - HTML visuals and the cleaned CSV are stored in the `output/` folder inside the VM.
   - `download_files.sh` helps retrieve these artifacts back to your local `src/output` directory.

---

## 🧐 Author
Chiranjit — Master’s Student in Information Systems @ Northeastern University, aspiring Data Engineer

---

## 📌 License
This project is open-source and licensed under the MIT License.

> If you found this project insightful or useful, please ⭐ the repository!


