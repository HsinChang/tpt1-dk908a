---
title: lab1_dk908a
author: ZHANG Xin
date: 18/09/2019
---
# Lab report fot TP "Introduction to Hadoop" of DK908a
## General advances in Lab
The whole lab is finished with desired "fancy" outputs, but some faults may still exist and improvements are still needed.

## How to run
### Compute Average
The execution of this function is not changed, after the configuration of hadoop and compilation, just run
```
hadoop jar code.jar tpt.dk908a.RunAverage /datasets/movie_samll /tmp/$USER
```

### Movie Recommendation
The file *MovieRecommadation.java* is modified in order to control which step of phases that you want to run with an **additional argument** at the end
#### frequent itemset
**Note**: the path **$PhaseX** here doesn't include the *.tX* suffix added by the program, you need to manually add the *.tX* suffix when verifying the output and don't type the suffix when running the program. 
To run `ExtractUserMovies`, run
```
hadoop jar code.jar tpt.dk908a.MovieRecommendation /datasets/movie_samll /tmp/$Phase1 1
```
To run `CountPairs`, run
```
hadoop jar code.jar tpt.dk908a.MovieRecommendation /tmp/$Phase1 /tmp/$Phase2 2
```
To run `SelectPairs`, run
```
hadoop jar code.jar tpt.dk908a.MovieRecommendation /tmp/$Phase2 /tmp/$Phase3 3
```
#### Fancy output
This part follows the frequent itemset calculation, the translation is done in two phases, firstly the $movie_{2}$(the movie recommended), and then $movie_{1}$.
So, firstly, run
```
hadoop jar code.jar tpt.dk908a.MovieRecommendation /tmp/$Phase3 /tmp/$Phase4 4
```
then run
```
hadoop jar code.jar tpt.dk908a.MovieRecommendation /tmp/$Phase4 /tmp/$Phase5 5
```
in order to get the final result, the result will be in the */tmp/$Phase5.t5* folder.

## Comments and Details
### Frequent itemsets
For the `ExtractUserMovies` phase, all movies IDs under the same user_id are put together, connected by commas. Then, in the `CountPairs` procedure, two embedded `for` loops are used in order to not counting the same pair twice.
Finally, in the `SelectPairs` procedure, the max pair is selected in a ergodic approach.
### Fancy Output
The general idea for this part is to use two **Reduce Side Join** phases. In the fourth phase, as we are replacing the id of $Movie_{2}$ with its name, the id of $Movie_{1}$ is still kept and check whether a string is numeric or not is rather esay, no additional keys are needed in the mapping procedure of this phase.
Yet for the fifth phase, it's no longer easy to distinguish the source file of the movies names, so new keys are introduced in the mapping processes.