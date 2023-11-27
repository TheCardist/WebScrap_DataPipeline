# Description
## Problem:
The company I work at had started using a new platform where most of the data pipelines we needed were completed except this one. We wanted some specific data which at the time could only be collected by the following steps.
1. Log into the vendor's website
2. Go to the destination page
3. Select a hotel
4. Click the download button
    
We wanted the exported data in a GCP table for analysis and to utilize for BI solutions but it wasn't something the vendor would get to until 2024.

## Solution:
This project automates those manual steps to download the files based on the previous optimization that occurred for each hotel that is on this platform. When the files are downloaded it then cleans the data, adds some new data points, and then uploads to appropriate GCP tables for later use in tools like Data Studio or Tableau.

## Technologies Used:
This project was written using Selenium, Subprocess (to write and read to GCP), Gsutil, and Pandas. It was originally written within the GCP Cloud Shell environment and later moved to a vertex AI Jupyter notebook and scheduled to run hourly via a Cawa job.

## Application Flow
- #### Getting the Hotel List
	- A query retrieves the hotels that are active on the platform.
	- This list of hotels is then compared against the optimizations.json file for the following:
		- Does the hotel exist in the JSON? If not, then it's added and this hotel is selected for downloads.
		- Does the hotel code + lst_optimization match what's in the file currently? If the optimization does not match then it means an optimization has occurred since the last run so this hotel would also be selected for downloads. 
	- If there are no hotels then that's the end of the program.
- #### Scrape Platform Website
	- Next, the application logins into the site and loops through each hotel within the Hotel List, logging into each property and downloading the report.
	- After the download I loop through the download directory to verify if that hotel has a file.
	- If a file fails to download then I remove the lst_optimization information from the JSON and it retries up to 3 times to re-download the file.
 - Assuming there were files downloaded then it moves to the next step.
- #### Report Cleaning
	- The downloaded reports are missing a few components that we want to have in the BQ table so I modify the files for each hotel to add those columns and data points then save the files as a modified version.
- #### Load to BigQuery
	- After that, I load the files to the appropriate tables
- #### Cleanup
	- Finally, I clean up the files in the local environment by copying them to a google cloud storage location, saving both the original and modified versions. Then I delete all the files I downloaded and modified, as well as some temp files I created in the process.

## Learnings
- This program was created in my local Windows environment using the google-bigquery package and accessing my personal GCP tables with a keys.json for credentials. I had never used the Cloud shell environment in GCP. After getting access to the appropriate work project_id in GCP, I moved this code to the Miniconda environment I set up. I found that the Google-bigquery client would no longer work due to permission issues; this also meant the keys.json was no longer necessary. This is when Subprocess with Gsutil was added to the code and refactored to support this new approach.

- Originally using Pandas to modify and save these modified documents to Google Cloud Storage which worked fine until a few days later it no longer did due to permission errors. I had to save these files to my local Cloud shell environment and then use a subprocess to copy them to the GCS location.

- Each day when logging into Cloud Shell I found that I had to reinstall the Google Chrome deb file. I corrected this by checking if Google Chrome was installed using 'which Google Chrome' and if it returned 0 then I knew I had to install it. This worked but I found I could edit the .customize-environment file for the Cloud shell and add the command there so it would install when it was booted up each time.

- The Cloud shell only offers 5GB of data storage and each hotel takes up roughly 10MB of storage. I only had a little over 1 GB to spare which will fill up quickly (10 hotels a day X 10MB per hotel) = 100 MB a day in storage. Because of this, I needed to implement a clean-up process at the end of the script to remove the downloads, modified files, and temp files I created. I don't know if this project will still be in use by the time it's 100 hotels a day but if so then I'll need to do a cleanup after each hotel at that point so the code would require refactoring.

- The first main implementation of this worked well but there were two main problems:
	- We distinguish in our database, hotels that are active on the website with an 'A' flag but sometimes a hotel is on the vendor site already but are not technically active yet. The business team asked to still get data for these 'Enabled' but not 'Active' hotels too.
  		- To solve this I implemented a solution that scrapes the hotel list off the vendor site instead of looking for 'A' hotels in the database. My account has access to all hotels that are available so this insures I get only the hotels I need each time.
 	- The application was taking roughly 20 minutes to process about 40-50 hotels at a time which was fine originally but it was taking longer than expected for the vendor to implement the permanent solution so this solution would need to be faster to handle hundreds of hotels.
  		- To solve this I refactored the entire approach and implemented multiprocessing for the scraping and file processing, which made massive performance improvements.
