# Finding Firefox Beta Subsets Resembling Release

Work related to the application of [Statistical matching](https://en.wikipedia.org/wiki/Matching_(statistics)) methods to finding subsets of Firefox Beta users that are representative of Release. These subsets can be utilized to forecast Release behavior _before_ it is launched to the populace. 

# Project
The primary project details are given in the [PRD](https://docs.google.com/document/d/1Ygz6MkudYHZjnDnD9Z97kUyFrvV3KGWsjXyPjddhHq0/edit?usp=sharing). The results and deliverables of the project [milestones](https://docs.google.com/document/d/1Ygz6MkudYHZjnDnD9Z97kUyFrvV3KGWsjXyPjddhHq0/edit#heading=h.lvb9l8gw2nee) are contained in the `perf_release_criteria` directory. 

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


