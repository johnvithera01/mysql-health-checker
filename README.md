# MySQL Ruby Monitor

A robust and customizable Ruby script for proactive health and performance monitoring of MySQL databases.

## Overview

This script provides continuous monitoring of critical, performance, and optimization metrics for MySQL, sending alerts via email when predefined thresholds are exceeded. It's designed to be flexible, easy to configure, and extend.

## Key Features

The `mysql_monitor.rb` executes checks at different frequency levels to ensure comprehensive coverage:

### High-Frequency Monitoring (Critical)
* **MySQL Service Status:** Verifies if the MySQL service is active and running.
* **CPU and Disk I/O Usage:** Monitors system resource spikes that can impact the database.
* **Active & Aborted Connections:** Alerts on the number of active connections nearing the configured limit and connections that were abruptly terminated.
* **Long-Running Queries:** Identifies and can optionally terminate queries exceeding a specified time limit.
* **Active Locks & Deadlocks:** Detects transactions waiting for persistent locks and deadlock events (from logs).
* **InnoDB Buffer Pool Hit Ratio & Dirty Pages:** Monitors buffer pool performance and the amount of modified pages not yet flushed to disk.
* **Replication Status & Lag:** Checks the health and delay of replication in Master/Slave environments.

### Medium-Frequency Monitoring (Performance)
* **InnoDB Table Fragmentation:** Identifies tables with high fragmentation that can affect performance.
* **InnoDB Row Lock Waits:** Monitors transactions waiting for row locks.
* **On-Disk Temporary Tables:** Alerts on excessive creation of temporary tables on disk, which degrades performance.
* **Open Tables Cache Efficiency:** Checks the effectiveness of the open tables cache.
* **Handler Operations:** Monitors handler operation metrics to identify full table scans (`Handler_read_rnd_next`) and inefficient index usage (`Handler_read_key`).
* **Network Throughput:** Verifies the volume of data sent and received by MySQL.

### Low-Frequency Monitoring (Optimization & Trends)
* **Disk Space:** Monitors free space on the MySQL data partition and the total database size.
* **Unused Indexes:** Identifies indexes that might be unnecessary and are consuming space.
* **Slow Query Analysis:** Processes the MySQL Slow Query Log to identify and help optimize problematic queries.
* **Table Size History Collection:** Saves historical table size data into an internal table for trend analysis and capacity planning.

## Requirements

* **Ruby:** Version 2.x or higher.
* **Ruby Gems:** `mysql2`, `mail`, `json`, `time`, `fileutils`, `yaml`. You can install them using Bundler:
    ```bash
    bundle install # Or 'gem install mysql2 mail' etc.
    ```
* **System Commands:** `mpstat`, `iostat`, `df`, `systemctl` (or `service`). Ensure that the `sysstat` package (for `mpstat`, `iostat`) is installed on your system.

## Configuration

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git](https://github.com/YOUR_USERNAME/YOUR_REPOSITORY.git)
    cd YOUR_REPOSITORY # Navigate to the project root folder
    ```
2.  **Edit the configuration file:**
    Open `config/mysql_monitor_config.yml` and adjust all settings for your environment. This includes:
    * Database credentials (host, port, database name).
    * Email settings for alerts.
    * Alert thresholds for each monitored metric.
    * Paths to MySQL logs (error log, slow query log).

    **IMPORTANT: Environment Variables for Credentials**
    For security, database credentials (`MYSQL_USER`, `MYSQL_PASSWORD`) and email password (`EMAIL_PASSWORD`) **MUST** be defined as **environment variables** in the environment where the script will be executed (and not directly in `mysql_monitor_config.yml`).

    Example of how to set them in your shell (temporary) or in your `~/.bashrc` / `~/.profile`:
    ```bash
    export MYSQL_USER="your_mysql_user"
    export MYSQL_PASSWORD="your_mysql_password"
    export EMAIL_PASSWORD="your_email_app_password" # For Gmail, typically an "app password"
    ```
    For production environments, consider using more robust solutions for secret management, such as `direnv`, `dotenv`, or dedicated secret managers.

3.  **Create log directories:**
    Ensure that the `log/` directory (and `config/` if it doesn't exist) exists in your project root, as specified in `mysql_monitor_config.yml`.

## How to Run

The script can be executed with different frequency levels by passing the level as an argument. It's ideal to schedule it using `cron` (see the next section).

```bash
# For high-frequency monitoring
ruby mysql_monitor.rb high

# For medium-frequency monitoring
ruby mysql_monitor.rb medium

# For low-frequency monitoring
ruby mysql_monitor.rb low

# To save table size history
ruby mysql_monitor.rb table_size_history
