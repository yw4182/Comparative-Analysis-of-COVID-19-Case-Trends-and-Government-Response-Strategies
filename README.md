# Comparative-Analysis-of-COVID-19-Case-Trends-and-Government-Response-Strategies

Entering the post-pandemic period of COVID-19, the world is gradually recovering from this economic and social crisis. Government responses significantly contributed to this recovery process. Strategies varied with different countries and regions and had different levels of effectiveness in controlling cases and deaths. Under this situation, controversies emerged regarding the balance of strictness level of government response and the effectiveness of lowering cases and deaths. Such controversies motivated us to uncover insights into the effectiveness of various government response strategies to COVID-19. The stringency index, a measure calculated from nine government response metrics, was selected as the variable to represent governments' responses. We used Hadoop technology to efficiently process and analyze large-scale COVID-19 datasets of 2020 with the support of Dataproc, used Tableau to visualize the processed data, and carried out analysis based on MapReduce results and Tableau graphs. From our analysis, both cases and deaths dropped considerably as the stringency index rose but the number of cases and deaths rose again under high stringency index circumstances. Thus, a higher stringency index produces an immediate effect on lowering the number of cases and deaths, which is a consistent result with many current works by experts. However, in the long-term perspective, a higher stringency index does not determine a lower overall mean of cases and deaths.

I use MR for my part
covid_policy_tracker.csv is original dataset
https://learn.microsoft.com/en-us/azure/open-datasets/dataset-oxford-covid-government-response-tracker?tabs=azure-storage

Data cleaning:
This dataset is very organized and formatted, so date or text formatting may not apply here. But
I did:
- drop whole column of load_date (not meaningful for our analysis)
- drop columns with uniform values
- drop rows if they have null value for more than half of the feature columns
by Clean.java, CleanMapper.java, CleanReducer.java, clean.jar (and respective class files) cleaned_data.csv is cleaned dataset

Data profiling:
Figure out that original file has 318763 records and cleaned has 302219 records by CountRecs.java, CountRecsMapper.java, CountRecsReducer.java, recs.jar (and respective class files)

Most of columns of this dataset is binary data or level data (e.g. the stay in home requirment: 0 -no measures 1 - recommend not leaving house 2 - require not leaving house with exceptions for daily exercise...), find their distinct values are not very meaningful since it has already stated, so I try to figure out distinct country name and count their respective data rows by DistinctColumnMapper.java, DistinctColumnReducer.java, DistinctColumn.java (and respective class and jar files). There are 186 countries in this dataset, and screenshot shows their respective row numbers. The result is country_count.csv I also did data visualization with Danyang Shi’s work: i found stringencyindex trend for Continent’s max/min death and cases countries. Those countries include Seychelles, South Africa, Brazil, Mexico, India, Iran, Russia, United Kingdom, Vanuatu, Australia. This shows in Data_Visualization.ipynb



Danyang's part: 
Sources:
1. ecdc_cases.csv
Covid cases and deaths records of Jan 2020 - Dec 2020
https://learn.microsoft.com/en-us/azure/open-datasets/dataset-ecdc-covid-cases?tabs=azure-storage

2. WPP2022_Demographic_Indicators_Medium.csv (Complementary)
World Population Projections 2022 (For demographic population of Jan 2021)
https://population.un.org/wpp/Download/Standard/CSV/

------------------------------------------------------------------------------

MR Code running order:
1. Clean
2. Val
3. Cont
X. Ppl2021

Code Output Directory (dataproc):
- dirToShareAccessProject
	- join_val_ppl
		- 000000_0 table derived from hive commands
	- project476
		- WPP2022_Demographic_Indicators_Medium.csv
		- ecdc_cases.csv
		- out_clean //output of MR clean
		- ecdc_cases_clean.csv // convert clean output to csv
		- out_val //output of MR val
		- ecdc_cases_val.csv // convert val output to csv
		- out_cont //output of MR continent
		- out_ppl2021 // output of MR ppl2021
		- ppl2021_Jan.csv // convert ppl2021 output to csv

------------------------------------------------------------------------------

etl_code (Cleaning with some Profiling and Formatting included):

1. Clean.java, CleanMapper.java
	- Keep columns continent, ISO3 country code, countries_and_territories, date, cases, deaths.
	- Add binary columns bin_cases, bin_deaths with 1 indicating there are cases/deaths
	- Format date
	- replace "_" in countries_and_territories with " "
	Output file: ecdc_cases_clean.csv converted from part-r-00000 in out_clean

2. Ppl2021.java, Ppl2021Mapper.java (Complementary)
	- Select rows that represents country/area and time 2021
	- Keep columns ISO3 country code, countries_and_territories, population on Jan 2021
	Output file: ppl2021_Jan.csv converted from part-r-00000 in out_ppl2021

------------------------------------------------------------------------------

ana_code:

1. Val.java, ValMapper.java, ValReducer.java
	- Take continent, ISO3, countries_and_territories as keys (so grouped by country)
	- Take cases, deaths as values
	I. Count number of observations
	II. Count distinct case/death number of each day (not applicable in analyzing)
	III. Sum up all case/death through out 2020
	IV. Calculate mean case/death of each day in 2020
	V. Calculate median case/death number of 2020 (not applicable in analyzing)
	VI. Calculate mode case/death number of 2020 (not applicable in analyzing)
	VII. Calculate standard deviation of case/death number of 2020
	Output file: ecdc_cases_val.csv converted from part-r-00000 in out_val
	Column name of output:
		continent [0]
		ISO3 [1]
		countries_and_territories [2]
		Number of observations [3]
		Number of distinct values [4]
		Cases sum [5]
		Cases mean [6]
		Cases median [7]
		Cases mode [8]
		Cases standard deviation [9]
		Deaths sum [10]
		Deaths mean [11]
		Deaths median [12]
		Deaths mode [13]
		Deaths standard deviation [14]

2. Cont.java, ContMapper.java, ContReducer.java
	- Take continent as keys
	- Take ISO3 country code, countries&territories, sum and std of cases and deaths as values
	1. Find minimum and maximum sum cases
	2. Find minimum and maximum std cases
	Output: part-r-00000 in out_cont
	What output represent is written explicitly.

3. join_val_ppl.txt
	- Hive code that is based ppl2021_Jan.csv and ecdc_cases_val.csv to tables
	- Need to be separately run in hive

	Specific steps:
	1. Create and load external table ppl2021 from ppl2021_Jan.csv
	2. Create and load external table val from ecdc_cases_val.csv
	3. Inner join two table on iso3 country code
	4. Calculate case-to-population and death-to-population ratio and add as columns
	5. Sort rows in descending order of case-to-population ratio
	Column names:
		1. iso3
		2. country_area
		3. population ppl
		4. sum cases
		5. case-to-population ratio
		6. sum deaths
		7. death-to-population ratio
	Output file is written as 000000_0 to directory join_val_ppl

------------------------------------------------------------------------------

data_ingest
1. Clean.sh & CleanClear.sh (Shell script to execute/reset MR Clean)
2. Val.sh & ValClear.sh (Shell script to execute/reset MR Val)
3. Cont.sh & ContClear.sh (Shell script to execute/reset MR Cont)
4. Ppl2021.sh & Ppl2021Clear.sh (Shell script to execute/reset MR Ppl2021)
5. runAll.sh & runAllClear.sh (Shell script to run all/reset all MR jobs)
6. join_val_ppl.txt (Hive commands used to generate 000000_0 as result)

------------------------------------------------------------------------------

profiling_code

**Profiling is integrated in these files

1. Clean.java, CleanMapper.java
	- Keep columns continent, ISO3 country code, countries_and_territories, date, cases, deaths.
	- Add binary columns bin_cases, bin_deaths with 1 indicating there are cases/deaths
	- Format date
	- replace "_" in countries_and_territories with " "
	Output file: ecdc_cases_clean.csv converted from part-r-00000 in out_clean

2. Cont.java, ContMapper.java, ContReducer.java
	- Take continent as keys
	- Take ISO3 country code, countries&territories, sum and std of cases and deaths as values
	1. Find minimum and maximum sum cases
	2. Find minimum and maximum std cases
	Output: part-r-00000 in out_cont
	What output represent is written explicitly.

------------------------------------------------------------------------------

Screenshots
1. Cleaning & Profiling: Clean_1 and Clean2 showing MR clean works
2. Analyzing 1: Val_1 and Val_2 showing MR Val works
3. Analyzing 2: Cont_1 and Cont_2 showing MR Cont works
4. Analyzing 3: Hive_combine showing hive codes works and successfully generate table with calculated results
** I didn't include screenshot of Ppl2021 b/c it is a complementary dataset brought in to help analysis, since Analyzing 3 is successful, MR Ppl2021 works

Lin's part: 

CodeMapper, CodeReducer, CodeDriver:
	Can be found in /Profiling/Lin Shue directory
	prints out all the distinct values in each columns, 
	since this is a covid dataset that includes date, state, number of positive and death, 
	the output in the SSH browser is extremely long and since this is not so useful later in our project, 
	I did not save it as a file. 

StatsMapper, StatsReducer, Stats:
	Can be found in /Cleaning/Lin Shue directory
	This mapreduce job first drop all rows that has 0.0 in 3rd and 4th column,
	which is the number of positive and death since they are pointless,
	then calculate the mean, median, and mode for each column
	screenshot of the output can be found inside the zipfile as well

CleaningMapper, CleaningReducer, Cleaning:
	Can be found in /Cleaning/Lin Shue directory
	this mapreduce job worked on the date format and deleted all the "-" to make the data numerical
	and then drop the rows that are outliers or have 0.0 as their positive/death number
	
Visualization Analytic Code
	Can be found in Screenshot/Lin Shue directory
	contains the screenshot of the output in NYU dataproc when running the data profiling code and the python code for visualizing the data using matplot.lib along with the screenshot of each graph. 

the input data and output data can both be found in the profiling folder
