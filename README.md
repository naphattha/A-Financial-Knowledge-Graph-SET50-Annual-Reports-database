# A-Financial-Knowledge-Graph-SET50-Annual-Reports-database
This project involve constructing two separate databases — one relational (MySQL) and one graph-based (Neo4j). These databases will store financial data related to SET50 companies, and you can then compare how each database performs in terms of query efficiency and accuracy for complex financial analysis.

---

## Prerequisites
- **Python**: Version 3.8 or higher  
- **Neo4j**: Community or Enterprise Edition  
- **MySQL**: Version 8.0 or higher  
- **Node.js and npm**: For frontend development (if applicable)
  
---

## Installation
Clone the Repository:
git clone https://github.com/naphattha/A-Financial-Knowledge-Graph-SET50-Annual-Reports-database.git
cd A-Financial-Knowledge-Graph-SET50-Annual-Reports-database

Set Up a Virtual Environment:
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install Dependencies:
pip install -r requirements.txt

---

## Set Up Databases:

MySQL: Create a database and import the necessary schema and data using mysql_set50.py.

Neo4j: Start the Neo4j server and run the neo4j_set50.ipynb notebook to populate the graph database.
