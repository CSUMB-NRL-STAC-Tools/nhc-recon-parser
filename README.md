# NHC Recon STAC Tools

## Project Overview

This project, developed as a senior project for the Naval Research Laboratory (NRL), provides a suite of tools designed to process and manage data related to National Hurricane Center (NHC) reconnaissance flights. The primary goal is to integrate and organize reconnaissance data into a SpatioTemporal Asset Catalog (STAC) format, enabling efficient discovery, access, and analysis of critical weather information.

## Purpose

The "NHC Recon STAC Tools" aim to enhance the accessibility and usability of historical hurricane reconnaissance data for researchers, forecasters, and analysts within the NRL and broader scientific community. By conforming to the STAC specification, the project facilitates interoperability and streamlines data workflows for atmospheric and oceanic research.

## Key Features

* **Data Ingestion:** Tools to parse and ingest TEMP DROP NHC reconnaissance data.

* **STAC Catalog Generation:** Functionality to convert processed data into STAC Items and Collections, adhering to STAC best practices.

* **Metadata Enrichment:** Capabilities to add rich, standardized metadata to reconnaissance assets.

* **Geospatial Indexing:** Integration with geospatial libraries for efficient querying and mapping of flight paths and observations.

* **Automated Processing:** Scripts for automating the cataloging process, reducing manual effort.

## Installation

To set up the project locally, follow these steps:

1. **Clone the repository:**
   git clone "re"
   cd nhc-recon-stac-tools

2. **Create and activate a virtual environment (recommended):**
    python -m venv venv

    On Windows:
    .\venv\Scripts\activate

    On macOS/Linux:
    source venv/bin/activate

3. **Install dependencies:**
    Ensure a virtual environment has been created and activated.

    pip install -r requirments.txt

    To install development dependencies do:

    pip install -r requirements-dev.txt