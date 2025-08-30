name: Odoo Inventory ETL

on:
  schedule:
    # Runs once daily at 00:15 UTC (3:15 AM Riyadh time)
    # A different time from your other workflow to avoid conflicts
    - cron: '15 0 * * *'
  workflow_dispatch: # Allows you to run it manually from the Actions tab

jobs:
  run-inventory-etl:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
      
      - name: Create credentials file from secret
        run: |
          echo '${{ secrets.GCP_SA_KEY }}' > gcp-key.json
        shell: bash

      - name: Run Inventory ETL script
        env:
          # This makes the gcp-key.json file discoverable by Google's libraries
          GOOGLE_APPLICATION_CREDENTIALS: 'gcp-key.json'
          # This securely provides the Odoo password to your Python script
          ODOO_PASSWORD: ${{ secrets.ODOO_PASSWORD }}
        run: python inventory_etl.py
