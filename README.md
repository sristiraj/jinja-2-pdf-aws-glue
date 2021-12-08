# jinja-2-pdf-aws-glue

Download the repo by clonign to local machine. Once done create a virtual env in python using command

virtualenv venv

Once done activate the virtualenv using command

For windows: venv\Scripts\activate

Once activated, run the command

pip install -r requirements.txt

Post this under venv\Lib\site-packages folder, zip all the files and folder as name 'site-packages.zip"

Upload the zip file to s3.

Create a glue job and add the uploaded zip folder s3 path in python library dependency.

Copy the app.py code to glue job code and add necessary parameters :

Upload the template.html file under resources folder to s3

INPUT_DATA_PATH - Data file which needs to be used to read data
OUTPUT_PDF_PATH - Path in s3 to generate pdf file
TEMPLATE_PATH - Path in s3 where template file is uploaded

Post this run the glue job to create pdf based on Jinja template defined in template.html

template.html, data under INPUT_DATA_PATH and logic for data processing can be modified and the codebase can be used as a general structure that can be used to generate pdf using glue in form of report
