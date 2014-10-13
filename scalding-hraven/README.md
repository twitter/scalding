# hRaven Extensions
This module includes additions to Scalding that make use of [hRaven](https://github.com/twitter/hraven) for querying job history.

## Reducer Estimation
Reducer estimators can include the `HRavenHistory` trait to get additional functionality for querying hRaven for past jobs.

For example, `RatioBasedReducerEstimator`, also in this module, uses hRaven job history to better estimate reducers based on the ratio of mapper-reducer input data.
