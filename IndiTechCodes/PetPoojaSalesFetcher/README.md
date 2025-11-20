# PetPooja Sales Fetcher

This console application fetches sales data from the PetPooja API and stores it into a local SQLite database (`sales_data.db`).

## Pre-requisites
- .NET 8 SDK 


## Setup



!. Install dependencies
   Microsoft.Data.SQLLite (version 10)

2. Set Environment variables Debug Properties

    
    :PETPOOJA_APP_KEY="..."
    :PETPOOJA_APP_SECRET="..."
    :PETPOOJA_ACCESS_TOKEN="..."
    :PETPOOJA_REST_ID="51wok2zxnsad"
    :PETPOOJA_FROM_DATE="2025-01-20 00:00:00"
    :PETPOOJA_TO_DATE="2025-01-20 23:59:59"
    

3.Run code

4. Result
    - `sales_data.db` will be created in the project directory.
    - The `sales_data` table will contain inserted rows.


## Pushing to GitHub
1. Create a new repo on GitHub.
2. Add remote and push:
   ```bash
   git init
   git add .
   git commit -m "Initial commit - PetPooja Sales Fetcher"
   git remote add origin https://github.com/<your-username>/<repo-name>.git
   git branch -M main
   git push -u origin main
