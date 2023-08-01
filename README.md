# MLB Analysis
Analyzing MLB players performance.

### Agenda

1. Purpose
2. Data Source
3. Project Architecture
4. Azure Databricks Set Up


#### Purpose
Analyzing pitchers and batters performance by following dimensions
* Pitchers
  1. Allowed HRs by left-hand batter V.S. right-hand batter.
  2. Allowed Hits percentage by left-hand batter V.S. right-hand batter.
  3. ERA by stadium.
  4. ERA by month.
  5. ERA by opposing team.

* Batters
  1. Batting Avg by left-hand pitchre V.S. right-hand pitcher.
  2. Batting Avg by opposing team.
  3. Batting Avg by stadium.
  4. Batting Avg by month.

#### Data Source
Data is retrieved from APIs provided by Sport Radar developer
https://developer.sportradar.com/

Though SportRadar already provides an in-depth analyzed data on each player in many dimensions by player profile API, this project retrieves raw data by calling play-by-play API, so that we can implement the entire data lifecycle from raw data retrieval, data processing and cleaning and loading data into dimensional models for further analysis.

This project runs on SportRadar trial account, so the rate of data retrieval is limited to 1 API call per second and 1000 API calls per month. Having said that, meaning current amount of data may not be a great scenario of using Spark. In order to showcase the implementation of Azure Data Factory, Azure Databricks and Azure Data Lake Storage Gen 2, we will still use databricks notebooks for ETL processes. 

#### Project Architecture
![Architecture](https://github.com/asd855280/mlb_analysis/blob/main/materials/Archi-pic.png?raw=true)
* **Storage**  
  All raw data and Unity Catalog managed tables data are stored in Azure Data Lake Storage Gen 2
* **Compute**  
  All ETL processes and computing execution run on Databricks interactive cluster, single node mode.
* **Orchestration**  
  Orchestrating and scheduling execution of Databricks notebooks from Azure Data Factory by implementing Link Services. 
* **Data Serving**  
  (Current)Utilize Power BI for analysis and data visulization.  
  (Pending)Utilize Spark ML for predicting each at bat result.

#### Dashboard
![Dashboard1](https://github.com/asd855280/mlb_analysis/blob/main/materials/Screenshot-batter.png?raw=true)

![Dashboard2](https://github.com/asd855280/mlb_analysis/blob/main/materials/Screenshot-batting-team.png?raw=true)
