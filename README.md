# Kiva Microfinance Data Platform

This repository contains the end-to-end data analytics pipeline for Kiva's public loan data. 

## Overview
This platform ingests external API data into PostgreSQL, streams database changes in near real-time via Debezium CDC and Redpanda (lightweight Kafka alternative) into ClickHouse, and performs analytics modeling using dbt. 

The entire pipeline is orchestrated using Dagster and monitored via Prometheus and Grafana.

## Setup Instructions
