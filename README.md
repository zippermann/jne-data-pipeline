# JNE Data Pipeline - Setup Guide

## Project Overview
This project creates a data pipeline to prepare and transform JNE logistics data from raw Excel/Oracle sources into a PostgreSQL database for dashboard consumption (Dashboard 141 & 117).

**Data Summary:**
- 36 tables
- ~601 total columns
- Key tables: CMS_CNOTE (main shipment), CMS_MANIFEST, ORA_ZONE, etc.

---

## Part 1: Prerequisites Installation (Windows)

### Step 1: Install Docker Desktop for Windows

1. **Download Docker Desktop:**
   - Go to: https://www.docker.com/products/docker-desktop/
   - Click "Download for Windows"

2. **Run the Installer:**
   - Double-click the downloaded `Docker Desktop Installer.exe`
   - During installation, ensure these options are checked:
     - ✅ Use WSL 2 instead of Hyper-V (recommended)
     - ✅ Add shortcut to desktop

3. **Complete Setup:**
   - Restart your computer when prompted
   - After restart, Docker Desktop will launch automatically
   - Wait for Docker to start (you'll see a whale icon in the system tray)

4. **Verify Installation:**
   - Open PowerShell or Command Prompt
   - Run: `docker --version`
   - Run: `docker-compose --version`
   - Both should display version numbers

### Step 2: Install Git for Windows

1. **Download Git:**
   - Go to: https://git-scm.com/download/win
   - Download will start automatically

2. **Install Git:**
   - Run the installer with default settings
   - When asked about PATH environment, select: "Git from the command line and also from 3rd-party software"

3. **Verify:**
   - Open new PowerShell window
   - Run: `git --version`

### Step 3: Install DBeaver Community Edition

1. **Download DBeaver:**
   - Go to: https://dbeaver.io/download/
   - Click "Windows (installer)" under Community Edition

2. **Install DBeaver:**
   - Run the downloaded installer
   - Follow the wizard with default settings

3. **First Launch:**
   - DBeaver will ask about downloading drivers - click "Yes"

---

## Part 2: Project Setup

### Step 1: Create Project Directory

Open PowerShell and run:

```powershell
# Create project folder
mkdir C:\Projects\jne-data-pipeline
cd C:\Projects\jne-data-pipeline

# Initialize git repository
git init
```

### Step 2: Create Project Files

Create the following folder structure:

```
jne-data-pipeline/
├── docker-compose.yml
├── .env
├── .gitignore
├── data/
│   └── raw/           # Place your Excel files here
├── scripts/
│   ├── init-db/
│   │   └── 01-create-tables.sql
│   └── etl/
│       └── load_data.py
├── config/
│   └── database.ini
└── README.md
```

Create folders:
```powershell
mkdir data\raw
mkdir scripts\init-db
mkdir scripts\etl
mkdir config
```

### Step 3: Copy Configuration Files

Copy the following files into your project directory (provided in this package):
- `docker-compose.yml`
- `.env`
- `.gitignore`
- `scripts/init-db/01-create-tables.sql`
- `scripts/etl/load_data.py`

---

## Part 3: Start the Environment

### Step 1: Start Docker Containers

```powershell
cd C:\Projects\jne-data-pipeline

# Start all services
docker-compose up -d

# Check if containers are running
docker-compose ps
```

You should see:
- `jne-postgres` - Running
- `jne-pgadmin` - Running

### Step 2: Verify PostgreSQL is Ready

```powershell
# Wait about 30 seconds, then test connection
docker exec -it jne-postgres psql -U jne_user -d jne_dashboard -c "SELECT version();"
```

---

## Part 4: Connect DBeaver to PostgreSQL

### Step 1: Create New Connection

1. Open DBeaver
2. Click the plug icon (New Database Connection) or go to Database → New Database Connection
3. Select **PostgreSQL** → Click Next

### Step 2: Enter Connection Details

| Field | Value |
|-------|-------|
| Host | localhost |
| Port | 5432 |
| Database | jne_dashboard |
| Username | jne_user |
| Password | jne_secure_password_2024 |

4. Click "Test Connection" - should show "Connected"
5. Click "Finish"

### Step 3: Access pgAdmin (Alternative UI)

1. Open browser: http://localhost:5050
2. Login:
   - Email: admin@jne.local
   - Password: admin123
3. Add Server:
   - Right-click "Servers" → Create → Server
   - General tab: Name: `JNE Pipeline`
   - Connection tab:
     - Host: `jne-postgres` (Docker network name)
     - Port: `5432`
     - Username: `jne_user`
     - Password: `jne_secure_password_2024`

---

## Part 5: Load Sample Data

### Step 1: Install Python Dependencies

```powershell
# In PowerShell
pip install pandas openpyxl psycopg2-binary sqlalchemy
```

### Step 2: Place Your Data File

Copy `JNE_RAW_COMBINED.xlsx` to:
```
C:\Projects\jne-data-pipeline\data\raw\JNE_RAW_COMBINED.xlsx
```

### Step 3: Run Data Loader

```powershell
cd C:\Projects\jne-data-pipeline
python scripts\etl\load_data.py
```

---

## Part 6: Push to GitHub

### Step 1: Create GitHub Repository

1. Go to https://github.com
2. Click "+" → New repository
3. Name: `jne-data-pipeline`
4. Keep it Private (recommended for company data)
5. Do NOT initialize with README (we already have one)

### Step 2: Push Your Code

```powershell
cd C:\Projects\jne-data-pipeline

# Add all files
git add .

# Commit
git commit -m "Initial setup: Docker + PostgreSQL + ETL pipeline"

# Add remote (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/jne-data-pipeline.git

# Push
git push -u origin main
```

---

## Useful Commands

```powershell
# Start containers
docker-compose up -d

# Stop containers
docker-compose down

# View logs
docker-compose logs -f

# Restart specific container
docker-compose restart jne-postgres

# Access PostgreSQL shell
docker exec -it jne-postgres psql -U jne_user -d jne_dashboard

# Remove everything (including data volumes)
docker-compose down -v
```

---

## Troubleshooting

### Docker Desktop won't start
- Ensure virtualization is enabled in BIOS
- Run PowerShell as Administrator: `wsl --install`

### Connection refused on port 5432
- Check if PostgreSQL container is running: `docker-compose ps`
- Check logs: `docker-compose logs jne-postgres`

### Permission denied errors
- Run PowerShell as Administrator
- Ensure Docker Desktop is running

### Data not loading
- Check file path is correct
- Ensure Excel file is not open in another program

---

## Next Steps

After completing this setup:
1. Verify data loaded correctly in DBeaver
2. Proceed to Data Transformation (Pandas scripts)
3. Set up Airflow for scheduling (Week 2)
4. Implement Kafka for real-time streaming (Week 3)
