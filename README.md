## First: Review Existing Unstructured Data and Diagram a New Structured Relational Data Model

  <p align="center">
  <img width="1000" height="500" src="https://github.com/hilmikilickaya/fetch_rewards/blob/master/img/Diagram.png">
  </p>
  
## Second: Write a query that directly answers a predetermined question from a business stakeholder

#### What are the top 5 brands by receipts scanned for most recent month?

 - I used SQLite dialect for my query. 
  
  ```sql
  SELECT  strftime("%Y-%m", r.dateScanned) AS month, b.name, COUNT(*) AS total
  FROM receipts AS r
  INNER JOIN brands AS b
    ON r.rewardsProductPartnerId = b.cpg_id
  GROUP BY strftime("%m", r.dateScanned), b.name
  HAVING strftime("%Y", r.dateScanned) = strftime("%Y", DATE())
  ORDER BY total DESC
  LIMIT 5;
  ```
## Third: Evaluate Data Quality Issues in the Data Provided
  ```python
  def create_df_normalize(json_file):
    '''
        Requires : json file
        Return : a structured data frame
    '''
    data = []
    for line in open(json_file, 'r'):
    data.append(json.loads(line))
    df = pd.json_normalize(data, )
    return df

  def rearrange_columns(df, df_name):
    '''
        Requires : a data frame, name of data frame
        Return : an organized data frame
      
    '''
    # Cleaning the column names
    column_names = list(df.columns)
    for i, name in enumerate(column_names):
        if (len(re.sub("[\w]+." ,"", name))) >= 2:
            column_names[i] = "_".join(name.split(".$")[:2])
        elif "$" in name or len(re.sub("[\w]+." ,"", name)) < 2:
            column_names[i] = name.split(".")[0]
        else:
            column_names[i] = name
    df.columns = column_names
    # Reordering the columns
  
    id_index = column_names.index("_id")
    new_order = column_names[id_index:] + column_names[:id_index]
    df = df[new_order]
  
    # Changing the data types
    for name in df.columns:
        if ("date" in name.lower()) or ("login" in name.lower()):
            df[name] = pd.to_datetime(df[name].astype(int, errors='ignore')/1000, unit='s')
        else:
            df[name]
    df.to_csv(f"{df_name}.csv", index = False)
    return df
   ```
#### Data Quality Issues

- There are duplicated values in users table
- Missing values in all three tables
- Wrong data types
- There are some receipts with multiplication entries


