# Flight Data Analysis: Airline On-Time Performance
## Project Overview
- Analyzed U.S. airline on-time performance using a dataset of flight details (1987–2008).
- Dataset includes 120 million records, ~1.6 GB compressed (~12 GB uncompressed).
- Objective: Identify causes of flight delays, predict patterns, and enhance operational efficiency.

## Data Analysis Approach
### Exploratory Data Analysis (EDA):
- Cleaned and preprocessed data for accuracy.
- Conducted statistical tests and visualized insights.

2.Algorithms Implemented:
- Flight Delay Analysis: Categorized delays to identify punctuality trends.
- Taxi Time Analysis: Assessed airport ground operation efficiency.
- Cancellation Reason Analysis: Ranked reasons for flight cancellations.
3.Workflow:
- Managed Hadoop jobs using Oozie for scalability and efficiency.

## Key Algorithms
1.Mapper Phase:
- Extracted fields: departure delay, arrival delay, airline code, etc.
- Calculated total delays and categorized flights as "on schedule" or "delayed."
2.Reducer Phase:
- Aggregated data to calculate on-time probabilities for airlines.
- Identified top and bottom-performing airlines based on punctuality.

## Challenges
- Scalability: Efficiently processing a 12 GB dataset.
- Resource Optimization: Ensuring high performance across distributed systems.
- Workflow Management: Seamlessly integrating multiple Hadoop jobs.
