# Mini Data Lake Project

This project implements a mini data lake solution for analyzing COVID-19 and YouTube trending data. It uses DuckDB as the data warehouse, Apache Airflow for scheduled ingestion, and provides a Streamlit-based visualization dashboard. The entire stack is containerized using Docker and orchestrated with Docker Compose.

## Project Structure

```
Création un mini data lake/
├── data/                      # Raw data storage
│   ├── corona/               # COVID-19 datasets
│   └── youtube/              # YouTube trending datasets
├── src/
│   ├── ingestion/            # Data ingestion scripts
│   │   ├── ingest_corona.py  # COVID-19 data ingestion
│   │   └── ingest_youtube.py # YouTube data ingestion
│   └── visualization/        # Visualization dashboard
│       └── app.py           # Streamlit dashboard application
├── minilake.duckdb          # DuckDB database file
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Docker Compose configuration
├── docker/                  # Dockerfiles for services
│   ├── Dockerfile.app       # Streamlit Dockerfile
│   └── Dockerfile.airflow   # Airflow Dockerfile
└── airflow/                 # Airflow DAGs and configs
    └── dags/                # Airflow DAG definitions
```

## Data Sources

### COVID-19 Data
The project uses several COVID-19 datasets stored in the `data/corona/` directory:
- `day_wise.csv`: Daily global COVID-19 statistics
- `country_wise_latest.csv`: Latest country-wise COVID-19 data
- `worldometer_data.csv`: Detailed COVID-19 statistics from Worldometer
- `usa_county_wise.csv`: County-level COVID-19 data for the USA
- `full_grouped.csv`: Comprehensive COVID-19 data by country and date
- `covid_19_clean_complete.csv`: Cleaned and complete COVID-19 dataset

### YouTube Data
YouTube trending videos data from different countries, stored in `data/youtube/`:
- Country-specific datasets (e.g., `USvideos.csv`, `FRvideos.csv`, etc.)
- Category mapping files (e.g., `US_category_id.json`, `FR_category_id.json`, etc.)
- Countries covered: CA, DE, FR, GB, IN, JP, KR, MX, RU, US

## Setup and Installation

### 1. Clone the repository
### 2. (Optional) Install dependencies for local development:
   ```bash
   pip install -r requirements.txt
   ```

## Running with Docker Compose

The recommended way to run the project is with Docker Compose. This will start Airflow (webserver and scheduler) and the Streamlit app, all using DuckDB as the database.

### Build and start all services:
```bash
cd "Création un mini data lake"
docker compose up -d --build
```

- The Streamlit app will be available at [http://localhost:8503](http://localhost:8503)
- The Airflow UI will be available at [http://localhost:8080](http://localhost:8080)

### Stopping services:
```bash
docker compose down
```

## Data Ingestion

Data ingestion is managed by Airflow, which runs the ingestion scripts on a schedule (see the Airflow DAGs in `airflow/dags/`). You can also run the ingestion scripts manually:

```bash
python src/ingestion/ingest_corona.py
python src/ingestion/ingest_youtube.py
```

- Ingestion scripts support incremental loading and deduplication.
- Data is stored in the `minilake.duckdb` file, which is shared between all services.

## Data Visualization Dashboard

The Streamlit dashboard (`app.py`) provides:
- Data Overview: List of tables, sample data, column info, row counts
- YouTube Analysis: Country-specific metrics, visualizations, engagement analysis
- COVID-19 Analysis: Global and country-level metrics, trends, and visualizations
- **Control Panel:** Upload CSV files to create new tables in DuckDB, and delete existing tables directly from the web interface.

### Control Panel Features
- **Upload CSV:** Users can upload a CSV file and specify a table name. The data will be inserted as a new table in DuckDB.
- **Delete Table:** Users can view all existing tables and delete any table with a single click.

This makes it easy to manage your DuckDB data lake directly from the Streamlit web app.

## Database Schema

### COVID-19 Tables
- `day_wise`, `country_wise_latest`, `worldometer_data`, `usa_county_wise`, `full_grouped`, `covid_19_clean_complete`
- Each table uses appropriate primary keys for deduplication

### YouTube Tables
- Tables named as `youtube_[country]videos` (e.g., `youtube_usvideos`)
- Primary key: `video_id`

## Dependencies

Key Python packages:
- `duckdb`: Analytical database
- `pandas`: Data manipulation
- `streamlit`: Web app framework
- `plotly`: Visualizations
- `apache-airflow`: Workflow scheduler (in Airflow containers)

See `requirements.txt` and `requirements-airflow.txt` for full lists.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 