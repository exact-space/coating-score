# Coating-Score

## Description

This repository contains the combined coating score for kilns to identify coating formation. The coating score considers various factors such as shell temperature, process parameters, and quality tags.

## Deployed Unit

The coating score system is deployed on **UTCL SDCW2**.

## Features

- **Shell Temperature Integration**: The system integrates shell temperature data to calculate the coating formation score.
- **Process Parameters**: It takes into account the relevant process parameters to assess coating formation.
- **Quality Tags**: The coating score also factors in quality-related tags to provide a comprehensive score.
- **Hourly Trigger**: The script triggers once every hour, calculates the coating score, and takes a maximum of 10 seconds to complete. Afterward, it enters a sleep mode until the next hour.
- **Data Fetching**: The script fetches four weeks' data from Kairos to perform the coating score calculation.
- **Publishing Data**: After calculating the coating score, the script publishes one data point every hour to both MQTT and Kairos.
