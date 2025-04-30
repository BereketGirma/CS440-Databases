# CS440-Databases
Utilizing PostgreSQL, MongoDB and Neo4j to manage course data and handle quering.

## 📦 Prerequisites 
- [Docker](https://www.docker.com/products/docker-desktop/)
- Git (for cloning the repository)
  
## 🛠️ Getting started
1. **Clone the repository**:
   ```bash
   git clone https://github.com/BereketGirma/AutoShift.git
   cd CS440-Databases
   ```
2. **Installing python libraries**
   ```
   pip install -r requirements.txt
   ``` 
3. **Running the Application on docker**
   > This will download images for PostgreSQL, MongoDB and Neo4j.  
   > Be sure to be in the root directory `CS440-Databases` when running the following command
   ```bash
   docker-compose up -d
   ```
4. **Run database.py**
   > This will parse the csv data and distrubute the data among the 3 databases
   ```bash
   python database.py
   ```

## Accessing the databases
### PostgreSQL
> Download [PostgreSQL](https://www.postgresql.org/download/)
### MongoDB
> Download [MongoDB Compass](https://www.mongodb.com/try/download/compass)
1. Add connection
   - url: mongodb://localhost:27017/
   - Name: CS440 (you can name it to what you desire)
2. Hit save and connect

### Neo4j
1. You can access Neo4j via local broswer: http://localhost:7474/browser/

