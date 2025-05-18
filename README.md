# -DM-Project-Barile-2208592
Project Proposal folder that includes the general info about the project.
Scripts, schemas, datasets and notes included.
Script files for the ETL Tools Process included in the ETL folder.
Initial external datasets include in IMDB datasets and spotify datasets folders.
Cleaned and Exctracted initial data included in cleansed datasets folder.
Joined datasets and fuzzy datasets folders contain useful data for the Transformation phase of the ETL Process.
Dimensions datasets folder includes the final dimensions to be loaded into the PostGres environment.
PostGres (PGAdmin) folder includes both create tables files and Olap queries for the analysis of the dw.
Schemas folder includes both DFM and Star schema.
PPTs folder that includes the final power point of the project.
Some datasets are not included due to the large size (see git ignore for more info).
To run the project correctly download first the two large imdb datasets about titles and names at the following link: https://developer.imdb.com/non-commercial-datasets/
Then run the ETL scripts in this exactly order: 1)Extract and Cleansing, 2)Joining, 3)Fuzzy matching, 4)Transforming, 5)Loading (execute the loading right after
the setup of the postgres environment through create tables files). The Extraction and Cleansing script needs the correct folders path to be executed correctly.
