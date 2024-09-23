# Fraud Ring Detection Service for Ride-Hailing Companies

This repository contains a system for detecting fraudulent ride patterns in a ride-hailing service by identifying fraud rings created by drivers, passengers, or both. The system uses non-supervised clustering techniques to find patterns of collusion among drivers and passengers based on ride data.

## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Files Description](#files-description)
- [How It Works](#how-it-works)
- [Contributing](#contributing)
- [License](#license)

## Introduction

Fraud rings in ride-hailing services occur when drivers and passengers collaborate to create fake or manipulated ride requests. This project implements a system that identifies these fraud rings by applying unsupervised clustering techniques to ride data.

The system uses the following key identifiers:
- `ride_id`
- `driver_id`
- `passenger_id`

It analyzes ride data and uses clustering algorithms (e.g., `scipy.sparse.csgraph.connected_components`) to identify suspicious groups of drivers and passengers who repeatedly engage in fraudulent behavior.

## Installation

To run the project, ensure you have Python installed on your machine.

1. Clone the repository:
   ```bash
   git clone https://github.com/vida-lashani/Fraud_Rings.git
   cd fraud-ring-detection
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up your database and ensure the tables mentioned in the `config.json` file are available. This system queries specific tables like `passengers`, `rides`, `suspected_rides`, etc., to detect fraudulent activity.

## Configuration

The configuration file `config.json` contains the necessary settings for running the detection system, including the names of the tables to be queried and thresholds for applying various rules. The file includes:

- **Table names**: Definitions of key tables used by the system.
- **Query thresholds**: Settings for defining fraud detection thresholds based on historical data and frequency of activity.
- **Frequency fraud rules**: Detailed rules to evaluate the frequency of rides, driver-to-passenger relationships, and passenger activity.

Example configuration:
```json
{
  "table_names": {
    "passengers": "passengers"
  },
  "fraud_table_names": {
    "passengers_table": "passengers",
    "driver_performance_table": "driver_performance",
    ...
  },
  "query_thresholds": {
    "passengers_lookback_days": 3,
    ...
  },
  "frequency_fraud_rules": {
    "rule1": {
      "more_count_freq": 11
    },
    ...
  },
  "db_name": "data"
}
```

Ensure that the `config.json` file is properly configured to match your database schema before running the service.

## Usage

1. Start the fraud detection service:
   ```bash
   python fraudring_main.py
   ```

2. The system will begin analyzing the ride data from your database using the parameters set in the configuration file.

3. Results: Suspicious rides will be flagged, and detailed reports will be generated.

## Files Description

- **`fraudring.py`**: Contains the main logic for detecting fraudulent activity. It evaluates the patterns of rides, identifying suspicious activities such as repetitive connections between drivers and passengers. It uses clustering algorithms to detect fraud rings.

- **`fraudring_main.py`**: The entry point to the system, responsible for initializing the process and calling relevant modules for fraud detection.

- **`driver_gang.py`**: Responsible for identifying gangs or colluding groups of drivers who frequently service the same set of passengers. Clustering methods are applied to detect such groups.

- **`passenger_gang.py`**: Analyzes passenger behavior to detect groups of passengers who are likely collaborating with drivers to perform fraudulent activities. Uses unsupervised learning techniques.

- **`rule_engine.py`**: Contains the rule engine that applies various fraud detection rules based on thresholds and frequency.

- **`query_handler.py`**: Manages database queries, retrieving data from the relevant tables (as defined in `config.json`).

- **`config.json`**: Configuration file where database tables, query thresholds, and fraud detection rules are defined.

## How It Works

The system works by:
1. **Data Collection**: Analyzes ride patterns in the database, such as `ride_id`, `driver_id`, and `passenger_id`.
2. **Rule Application**: Applies a series of rules and thresholds to detect abnormal behavior.
3. **Clustering and Graph Theory**: Uses non-supervised clustering algorithms such as `connected_components` from the `scipy.sparse.csgraph` module to detect fraud rings. This method identifies connected subgraphs of drivers and passengers who show suspicious interactions.
4. **Fraud Ring Detection**: Identifies potential fraud rings by examining:
   - Passenger-to-driver relationships.
   - Ride frequency.
   - Unique passenger and driver rates in a certain time window.
   - Cities where fraudulent activity may be concentrated.

By applying these clustering techniques, the system efficiently detects groups of drivers and passengers acting fraudulently, without the need for labeled data.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
