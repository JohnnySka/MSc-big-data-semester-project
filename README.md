## MSc - Data Science (University of Nicosia)
## Big Data Management and Processing - Semester Project

This repository contains the notebook files related to our semester project for the Big Data Management and Processing module, part of the MSc in Data Science from the university of Nicosia.

The focus of the project was to adopt a Big Data solution for a chosen dataset/problem, and our dataset of choice was a large set (13.5M rows) containing crime data from London, spanning from 2008 to 2016. The dataset can be found on the following link (Kaggle):

https://www.kaggle.com/datasets/jboysen/london-crime

The big data problem we are adressing is mainly Volume.

This repo contains 4 jupyter notebooks:

### Big Data - Final Project (part 1)
This notebook contains a preliminary simple showcasing of one idea that can be adopted, which would involve transformation of the initial data to document format using Google's Firestore. We implement a function to perform these actions, saving the data on Firestore as a document collection and then we also perform a few indicative queries to complete this introductory part. Queries performed on Firestore through the local notebook were admittedly extremely computationally heavy, and we had to limit the outputs so that the queries did not use the entire dataset.

### Big Data - Final Project (part 2)
This notebook contains our main approach/solution.
The first step is to turn the data into JSON format (which is actually done at the end of the notebook in part 1).
In this second part of the project, the JSON file is loaded into a Spark Dataframe. We use RDDs and Spark Dataframes to analyse our data and get insights. 
We use python's time library to count the time each MapReduce function takes on RDDs to produce our results and we also do the same for the equivalent query
on the Dataframes.

For the final part of this section, we also perform two hypothesis tests, namely a Chi-Square test of Independence (Major Crime Categories vs Boroughs) and a T-Test (comparing the top two boroughs for total crime means). The code for these tests is separately used to create two .py executables, which are then uploaded to the designated GCS bucket and used in the respective Dataproc job sessions (along with the JSON data file). The Dataproc cluster we provisioned consisted of 1 master and 2 worker nodes.

For simplicity reasons, and since we were able to store the JSON file locally as well, the rest of the initial code (except for the statistical tests) was run locally and the results we see are those of the local run. 
In order to run the MapReduce functions on RDDs and the queries on Dataframes on Dataproc, some lines of code must be added, similar to the ones on the Hypothesis testing .py files that can be found in the repository (<u>dataproc_chi_squared</u> and dataproc_t_test).

Included in the repo are also a few screenshots of the test results and the job sessions, indicative of the Dataproc enviroment.

Finally, in order to visualize some of the aforementioned results and also to visually enrich the dataset exploration a little bit more, we created a Looker Studio dashboard, which can be found here:

https://lookerstudio.google.com/reporting/0cc479e4-a2db-460e-9545-244115576a9b

