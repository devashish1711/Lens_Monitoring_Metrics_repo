# Lens Monitoring Metrics

This project is a **Python-based monitoring tool for Google Cloud Platform (GCP)** that allows users to fetch cloud resources and configure monitoring metrics directly from a single application.

The tool authenticates using a **GCP Service Account JSON key**. Once authenticated, it automatically connects to the **Google Cloud APIs** and retrieves available resources from the user's GCP account.

Users can then select resources and configure monitoring metrics directly from this application.

---

# Project Logic

The main idea behind this project is to simplify **cloud monitoring configuration**.

Instead of manually navigating the GCP console to configure metrics, this application allows users to:

* Authenticate to GCP
* Fetch all available resources
* Select the resource type
* Configure monitoring metrics
* Send configuration requests to the **GCP Monitoring API**

The application acts as a **central monitoring interface for GCP resources**.

---

# Authentication Process

The tool uses **Service Account authentication**.

Steps required before running the script:

1. Go to **Google Cloud Console**
2. Navigate to:

```
IAM & Admin → Service Accounts
```

3. Create a new **Service Account**
4. Grant required roles such as:

   * Viewer
   * Monitoring Viewer
   * Compute Viewer
5. Generate a **JSON Key**
6. Download the JSON key file

This JSON key is required by the script to authenticate with the GCP APIs.

---

# How Authentication Works

The user must provide the **service account JSON file** to the application.

Example:

```
service-account-key.json
```

Once provided, the Python script loads the credentials and connects to **Google Cloud**.

After authentication, the program can access:

* Cloud Monitoring API
* Compute Engine API
* Kubernetes API
* Networking API
* Cloud SQL API

The script then automatically fetches available resources from the GCP account.

---

# Resource Categories Supported

Currently the application can fetch monitoring data for four main resource types:

* **Virtual Machines (VM)**
* **Networking**
* **Databases (Cloud SQL)**
* **GKE Clusters**

These resources are fetched automatically from the GCP project once authentication is successful.

---

# Monitoring Metrics

After selecting a resource type, the application allows the user to configure monitoring metrics.

### VM Metrics

* CPU utilization
* Memory usage
* Disk usage
* Network traffic

### Network Metrics

* Firewall traffic
* Network throughput
* Packet count

### Database Metrics

* CPU usage
* Connection count
* Query performance

### GKE Metrics

* Node CPU usage
* Pod usage
* Cluster health metrics

---

# Custom Metrics Support

The tool also allows users to configure **custom monitoring metrics**.

Users can define their own metrics and send monitoring requests to the **GCP Monitoring API**.

This makes it possible to monitor **application-level metrics in addition to infrastructure metrics**.

---

# How the Application Works

### Workflow

1. User provides **Service Account JSON key**
2. Python script authenticates with GCP
3. Script fetches available resources from the project
4. User selects resource type
5. User configures monitoring metrics
6. Script sends request to **GCP Monitoring API**
7. Metrics are retrieved and displayed

This allows users to perform **monitoring configuration from a single application instead of manually using the GCP console**.

---

# Files in the Repository

### metrics.py

Main script that handles:

* Authentication
* Resource discovery
* Metrics configuration
* Monitoring API requests

### requirements.txt

Contains required Python dependencies.

### .gitignore

Prevents sensitive files such as **service account JSON keys** from being pushed to GitHub.

---

# Running the Project

Clone the repository:

```bash
git clone https://github.com/devashish1711/Lens_Monitoring_Metrics_repo.git
```

Navigate to the directory:

```bash
cd Lens_Monitoring_Metrics_repo
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the script:

```bash
python metrics.py
       or
python metrics.py --sa-file ccd-poc-project-3847234sdhdf.json     ##this is .json service key fie##
```

Provide the **service account JSON key** when prompted.

The application will then fetch resources and allow metrics configuration.
