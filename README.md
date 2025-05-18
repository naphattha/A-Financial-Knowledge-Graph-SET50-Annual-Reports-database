# A-Financial-Knowledge-Graph-SET50-Annual-Reports-database
This project involve constructing two separate databases — one relational (MySQL) and one graph-based (Neo4j). These databases will store financial data related to SET50 companies, and you can then compare how each database performs in terms of query efficiency and accuracy for complex financial analysis.

---

## Prerequisites
- **Python**: Version 3.8 or higher  
- **Neo4j**: Community or Enterprise Edition  
- **MySQL**: Version 8.0 or higher  
- **Node.js and npm**: For frontend development (if applicable)
- **Data Files**: FilteredFinancialData.json and FilteredEODData.json
  
---

## Installation
Clone the Repository:
```bash
git clone https://github.com/naphattha/A-Financial-Knowledge-Graph-SET50-Annual-Reports-database.git
cd A-Financial-Knowledge-Graph-SET50-Annual-Reports-database
```

Set Up a Virtual Environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install Dependencies:
```bash
pip install -r requirements.txt
```

---

## Required Data Files:

You need two JSON files from the SET API, placed in the project directory:

FilteredFinancialData.json: financial statement data

FilteredEODData.json: end-of-day stock prices

---

## Set Up Databases:

- **MySQL**: Create a database and import the necessary schema and data using mysql_set50.py.

1.Start your MySQL server.

2.Create a new database (e.g., SET50_DB).

3.Run the script to populate the database:
```bash
python mysql_set50.py
```

- **Neo4j**: Start the Neo4j server and run the neo4j_set50.ipynb notebook to populate the graph database.

1.Start your Neo4j Desktop or Neo4j server.

2.Open and run the neo4j_set50.ipynb Jupyter Notebook to populate the graph database.


- **Extending Beyond SET50**
To add companies not in the original SET50:

Append their symbol-name entries to the company_names list or database table.

Ensure their financial and EOD data exists in the JSON files.

Rerun the import scripts.


