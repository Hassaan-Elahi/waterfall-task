# Waterfall Prospector
This Python script automates the process of fetching contacts 
of companies using the Waterfall API. It takes a CSV file containing domain names and a filter expression as inputs, filters the contacts based on a filter expression provided, and saves the retrieved contact information in both a CSV file and a database.

## Instructions
1. Clone the repository using ``git clone git@github.com:Hassaan-Elahi/waterfall-task.git
``
2. ``cd`` into the directory using ``cd waterfall-task``
3. Create a new virtual environment using ``python3 -m venv venv``
4. Activate the virtual environment using ``source venv/bin/activate``
5. Install the dependencies using ``pip install -r requirements.txt``
6. Change the variables in the ``.env`` file to your own credentials
7. Apply the migration by running ``alembic upgrade head``. It will create all the tables in the database.
8. Run the script ``python main.py input.csv manager``. The first argument is the path of the input file and the second argument is the filter expression.


## Preqrequisites
- Python 3.x
- PostgreSQL
- Waterfall API credentials