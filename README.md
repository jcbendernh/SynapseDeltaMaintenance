# SynapseDeltaMaintenance

While much of the Delta table maintenance is handled in Fabric, there are many customers still using Azure Synapse.  The purpose of this repo is to provide customers with a simple solution to optimize and vacuum these tables in an automated fashion.

If you created a delta table in a Mapping Data Flow via Pipelines, there is a good chance that delta table is not registered within your spark pool.  We must first ensure that the delta table is listed there as it is the spark pool that performs the optimize and vacuum actions.

To do so, download the <b>Delta Table Maintenance-Manual</b> Jupyter notebook and import it into your Synapse workspace and follow the commands in the notebook.  This shows you how to 
1)	Verify that your delta table exists.
2)	Add it to the Spark catalog as an external table if it does not exist.
3)	Run the optimize and vacuum commands.
Make sure all your delta tables are created and show in the listing before proceeding to the next steps.
<br>&nbsp;<br>

<b>Prerequisites for Delta Table Maintenance pipeline.</b>
-	Spark Pool created to be used with automated runs.
-	Create a separate container called Other on your synapse storage account associated with the workspace.  See the screenshot for reference…
![picture alt](/img/1.png)

Next we will import the pipeline and it’s corresponding notebooks.  We must import the notebooks first.  Download and import the <b>Delta Table Maintenance-1</b> and <b>Delta Table Maintenance-2</b> notebooks into the Synapse workspace.  Once imported, please attach them to a Spark Pool in your Synapse Workspace.  

<b>NOTE:</b>  We recommend setting up a separate Spark Pool for automated jobs.  Thus, one spark pool for interactive workloads and another for automated jobs.

Next download the Delta Table Maintenance zip file from this repo and import it into pipelines in your Synapse workspace.  To do so, go to Integrate and click + and choose <b>Import from pipeline template.</b>
![picture alt](/img/2.png)

During the import process, you have to declare the Linked service, use the default workspace Linked Service…
![picture alt](/img/3.png)

Once imported, you should see the Pipeline, click Save/Commit.
![picture alt](/img/4.png)

There are a few verification steps to review and this also give you a better understanding of how the pipeline functions.

For the <b>ShowTables</b> notebook activity, under Settings, verify that Delta Table Maintenance-1 is listed and select your Spark Pool for automated jobs.
![picture alt](/img/5.png)

Under your Lookup activity, verify your wildcard file path.  This is where the spark Tables csv listing was written o in the previous step.  This activity will read that csv and execute a ForEach activity for each delta table listed.
![picture alt](/img/6.png)

On the settings tab of the ForEach activity, you will notice Sequential is checked.  This means the pipeline will take longer as only one notebook at a time will run be a lower executor size is needed.  If this is deselected, additional spark pools will be generated and there will be parallel runs which could necessitate a larger spark pool to be executed for this activity.  
![picture alt](/img/7.png)

Under the <b>Delta Table OPTIMIZE AND COMMAND</b> notebook activity, click on Settings to verify that Delta Table Maintenance-2 is listed and select your Spark Pool for automated jobs.

If you expand the Base parameters, this shows the parameters that are passed to the Delta Table Maintenance-2 notebook.  Do not modify these.
![picture alt](/img/8.png)

Once finished click Save/Commit.
You can manually run your pipeline or you can add a Trigger for it to run on an automated basis.  For more on this topic check out https://learn.microsoft.com/en-us/azure/data-factory/how-to-create-schedule-trigger?tabs=data-factory. 
Also, you can review the following article for a more complex maintenance job: https://techcommunity.microsoft.com/t5/azure-synapse-analytics-blog/strengthen-delta-lake-in-synapse-with-auto-maintenance-job/ba-p/3737161#:~:text=Maintenance%20Needed%20for%20Delta%20Lakes%201%20The%20%E2%80%9C,size%20is%20performed%20by%20the%20Vacuum%20command.%20
