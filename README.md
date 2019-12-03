# Finding Firefox Beta Subsets Resembling Release

Work related to the application of [Statistical matching](https://en.wikipedia.org/wiki/Matching_(statistics)) methods to finding subsets of Firefox Beta users that are representative of Release. These subsets can be utilized to forecast Release behavior _before_ it is launched to the populace. 

# Proof-of-concept
Initial proof-of-concept work is contained in the `poc` directory. 
The [MatchIt](https://cran.r-project.org/web/packages/MatchIt/vignettes/matchit.pdf) library in R was used for matching.

* `data_prep`: contains pyspark scripts (converted from Databricks notebooks) that created datasets from Firefox telemetry pipeline.
   - `Beta_Release_Matching_Perf_Metrics_POC.py`: data munger for the [initial analysis](https://metrics.mozilla.com/protected/cdowhygelund/beta_subset_release.html#tl;dr).
   - `Beta_Release_Matching_Perf_Metrics_Validation.py`: data munger for the [validating analysis](https://metrics.mozilla.com/protected/cdowhygelund/beta_subset_release_validation.html)
* `analysis`: contains R Markdown, scripts, and final rendered html reports. 
   - `modeling_poc.Rmd`: grunt exploratory work regarding initial statistical modeling efforts.
   - `report_poc.Rmd`: final R Markdown report regarding [initial statistical modeling](https://metrics.mozilla.com/protected/cdowhygelund/beta_subset_release.html#tl;dr).
   - `report_validation.Rmd`: final R Markdown report regarding [method validation](https://metrics.mozilla.com/protected/cdowhygelund/beta_subset_release_validation.html).    


