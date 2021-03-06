## Mile High Route Analysis
![Graph Map](https://github.com/ccvandusen/Flight-Route-Analysis/blob/master/images/AIRWAY.jpeg)

This repo is a pipeline that takes data from the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/), cleans and labels the data, and then trains a Random Forest Classifier to make predictions on route discontinuations.

### Introduction:

For regional airports and the populations they service, direct routes to major hubs is a big deal. Airline carriers will frequently test out operating flights from their major hubs to new airports, just to close the route later if it underperforms. For travel agencies, people who travel for business, and anyone who desires a specific direct connection between less travelled airports, predicting the future route network could be very valuable.

### The Data:

The data for the model consists of 6-7 million flights / year from BTS's On-Time Performance dataset and monthly aggregates of passenger and airplane statistics from BTS market segment dataset. The classifier has currently been trained on data from 2004-2012, with a test set of new routes from 2013. However, the databases are active and both update on a monthly basis so the model can be retrained on more recent data to produce better results. The data did not include route labels or closure indicators, so I had to engineer them for the model to predict on. To this end I used an AWS EC2 instance and stored all my engineered features on an S3 instance. You can access sample data that has already run through my raw data clean script from 2004-2008 [here](https://drive.google.com/drive/folders/0B-Is4Z7_0qg_X3NoeTdEX3dwZG8?usp=sharing).

### The Model:

One of the biggest issues for building a classification model on this data was the temporal nature of the data. In particular, many routes only ran for a few months before being discontinued, while many more have several years of data.

![Route Distributions](https://github.com/ccvandusen/Flight-Route-Analysis/blob/master/images/closed-route-length-distribution.png)

After attempting several methods of imputation and feature engineering to capture the temporal nature of the data, the final model aggregates the most recent six months of data to train on. This method produced the best results, because indicating the length of time a route has flown is a leakage issue, and this aggregation strategy nicely mitigated the problem.

### Results:

After establishing a baseline 83% validation accuracy, with all features thrown into the model, I pruned the training variables down to the best performing 21 variables for the final model. Of the 21 chosen features, the ones with the highest feature importance were `average fill`, which is the average number of seats taken for a flight on a given route, and the `average flight delay`, or the difference between the scheduled flight time and the actual flight time. You can view my feature importances here: ![Graph Map](https://github.com/ccvandusen/Flight-Route-Analysis/blob/master/images/feature-importances.png)
I produced an ***89.5% accuracy score***, with a little over 85% on both precision and recall.

### What Next:

I believe this model could be greatly improved with socioeconomic data, which is plentiful in the public realm. One exciting use of the data I have collected, which I intend to do for my analysis next, is predict new routes opening using link prediction. There is a lot of new research in the area, and using the BTS' market and segment data to model traffic flow, as well as my models results for each route, I could model the entire network's evolution from one year to the next. Ultimately, using D3 or a similar dynamic visualization framework, I would create an interactive graph that predicts how the network evolves over time.
