## Data Dictionary for Current Delta Tables

covid_cases <br>
 |-- **date** (string): Date of case enumeration, in YYYY-MM-DD <br>
 |-- **county** (string): US county being enumerated <br>
 |-- **state_full** (string): full name of US state being enumerated <br>
 |-- **fips** (double): Unique 5-digit code that identifies a given county in the United States <br>
 |-- **cases** (long): Total count of positive cases reported on the given date <br>
 |-- **deaths** (long): Total count of deaths reported on the given date <br>
 |-- **code** (string): Two letter abbreviation for state_full <br>
 
 
 covid_tests <br>
 |-- **date** (string): Date of case enumeration, in YYYY-MM-DD <br>
 |-- **state** (string): full name of US state being enumerated <br>
 |-- **fips** (double): Unique 2-digit code that identifies a given county in the United States <br>
 |-- **positive** (long): Total count of positive test results reported on the given date <br>
 |-- **negative** (long): Total count of negative test results reported on the given date <br>
 |-- **deaths** (long): Total count of deaths reported on the given date <br>
 |-- **dateChecked** (string): original date of test, if different from current date <br>
 |-- **totalTestResults**: (double): Sum of positive and negative tests <br>
 |-- **deathIncrease** (long): Day over day increase in deaths <br>
 |-- **hospitalizedIncrease** (long): Day over day increase in hospitalizations <br>
 |-- **negativeIncrease** (long):  Day over day increase in negative tests <br>
 |-- **positiveIncrease** (long): Day over day increase in positive cases <br>
 |-- **totalTestResultsIncrease** (long): Day over day increase in total tests <br>
 |-- **hospitalized** (double): number of people hospitalized in a given day <br>
 |-- **pending** (double): number of tests awaiting results <br>
 
 
 populations <br>
 |-- **Id** (string): Unique id code given to county/state combination <br>
 |-- **Id2** (long): Fips code for couny <br>
 |-- **County** (string): Name of county <br>
 |-- **State** (string): Name of state <br>
 |-- **pop_estimate_2018** (long): Total estimated population of county (in 2018) <br>
