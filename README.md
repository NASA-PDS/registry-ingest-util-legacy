# pds-registry-ingest
Utility script to load data in the PDS registry in parallel using the pds-registry-app.

## Prerequisites


- **Python3**  is required.
- pds-registry-app 


## User quickstart

    pip install -r requirements.txt

Copy config template and update it

    cp pds_registry_ingest.ini.default pds_registry_ingest.ini

Edit the new conf file

Run:

    python pds/registry/ingest.py <dir of the archive>




