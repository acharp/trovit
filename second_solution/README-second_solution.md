# Sort deals in cars classified ads

This second solution intends to just reimplement the exact same logic as the one in the first solution but using a totally different technological stack : Apache Spark jobs written in Scala.  

Even with just a toy project like this one, crafting these two solutions made me notice some advantages of the Spark/Scala stack over a more classic SQL one glued with some Python scripts :
* Infering the schema from the json instead of having to manage it myself
* Not having to type everything at the output as everything remains typed all along the pipeline thanks to scala
* The code is much much much shorter
* Possibility of writing unit test on the pipeline logic which was not possible in sql where only end to end tests are workable there  

Also to note, the results of this solution are not as good as for the first one, both in terms of performance and logic.  
As I'm not so skilled at optimizing Spark jobs the entire process is much slower (~ 5minutes) than the Redshift one. But I ran it on 2 cores on my laptop whereas the Redshift solution was running on a cluster.
Concerning the logic, I don't filter all the outliers as neatly as I'm doing in the Redshift solution so some results suck (ex: We identify some Good deals whereas the price of the classified ad is 0.0)


## The project

* `Main.scala` Contains all the Spark jobs.
* `TestMain.scala` Some unit tests.
* `test-sample.json.gz` The input files for the unit tests.
* `sample-insights.json` A sample of the final result I output.

As the logic implemented is mostly the same as for the first solution I won't reexplain it here, I just directly commented inside the scala files to give.  
The same about the third question and how to push our insights in real-time to our users. At the end of the pipeline I would just send the json records to a stream and then do the exact same steps. So you can just refer to the README-first_solution.md.
