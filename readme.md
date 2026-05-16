# Real-time Anomaly Detection on Streaming Data

This project is an end-to-end system for detecting anomalies in real-time streaming data. It simulates a stream of IoT sensor data, processes it through an Apache Kafka pipeline, uses a machine learning model to identify unusual patterns, and is designed to visualize the results on a live dashboard.

The entire multi-service application is containerized using Docker and managed with Docker Compose for easy and reproducible setup.

---

### Technology Stack

*   **Programming Language:** Python 3.9
*   **Data Streaming:** Apache Kafka
*   **Orchestration:** Docker & Docker Compose
*   **Machine Learning:** Scikit-learn
*   **Data Handling:** Pandas

---

### Prerequisites

Before you begin, ensure you have the following installed on your local machine:

*   [Docker Desktop](https://www.docker.com/products/docker-desktop/) - To run the containerized application.
*   [Git](https://git-scm.com/downloads) - For version control and cloning the repository.

---

### Project Setup and Execution

Follow these steps to get the application running. (Its still in the process of being built so 'd recommend not cloning as of now)

**1. Clone the Repository**

First, clone the project from your Git repository to your local machine:

```bash
git clone <your-repository-url>
cd realtime-anomaly-detection
```

**2. Build and Run the Services**

The entire application stack is defined in the `docker-compose.yml` file.

To build the custom Python image and start all services (Zookeeper, Kafka, and the Python application), run the following command from the project root directory:

```bash
docker-compose up --build -d
```

- `up` → Creates and starts all containers defined in `docker-compose.yml`
- `--build` → Rebuilds the Docker image before starting the containers
- `-d` → Runs the containers in detached mode (background execution)

Use the `--build` flag whenever you modify:

- `Dockerfile`
- `requirements.txt`

---

**3. Verify the Services are Running**

To check the status of the running containers, use:

```bash
docker-compose ps
```

You should see containers like:

- `zookeeper`
- `kafka`
- `python-app`

with status `Up` or `Running`.

---

**4. Stop the Application**

To stop and remove all running containers, networks, and volumes created by Docker Compose, run:

```bash
docker-compose down
```

This cleanly shuts down the entire application stack.