# Projeto Airbnb Rio de Janeiro
## Context
On Airbnb, anyone who has a room or a property of any type (apartment, house, cottage, inn, etc.) can offer their property to be rented by the day.

You create your host profile (person who offers a property for rent per day) and create your property advertisement.

In this ad, the host must define the characteristics of the property as completely as possible, in order to help landlords / travelers to choose the best property for them (and in order to make their ad more attractive)

There are thousands of possible customizations for your ad, from the minimum daily amount, price, number of rooms, to cancellation rule, extra fee for extra guests, requirement for verification of landlord identity, etc.

## Objective
Build a price forecasting model that allows an ordinary person who owns a property to know how much to charge for their property's daily rate.

Or, for the common landlord, given the property he is looking for, help to know if that property has an attractive price (below the average for properties with the same characteristics) or not.

The databases were taken from the kaggle website: https://www.kaggle.com/allanbruno/airbnb-rio-de-janeiro
The databases are the prices of the properties obtained and their respective characteristics each month.
Prices are given in reais (BRL)
The databases are from April 2018 to May 2020, with the exception of June 2018 which does not have a database

## Code
In the file "Solution Airbnb Rio" you will find all the code made with all the detailed steps. Database import, treatments, feature analysis along with the exclusion of outliers, forecast metrics, model analysis, choice of forecast model and, finally, model training.
In the file "DeployProjetoAirbnb" is the deployment of this model to make it usable.
In order to use it, it is necessary to take the following steps:

- 1st: Download the files "Solution Airbnb Rio" and "DeployProjetoAirbnb" both in .ipynb format
- 2nd: Download the code in .py format (File/Download as/Python(.py))
- 3rd: Open anaconda's command prompt and type streamlit run DeployProjetoAirbnb.py

Once this is done, you will open a page in the browser ready to enter the values and make your property price forecast. 