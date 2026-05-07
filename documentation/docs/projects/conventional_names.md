# Conventional names

## Notebooks
```
Example of sctrcuture from file:

(Number_of_script)_(Process from Pipeline)_(Layer/source)_(target)
```

```
├── 01_EL_postgres_to_minio_landing_parquet.ipynb
├── 02_EL_minio_landing_to_bronze_delta.ipynb
├── 03_Transform_bronze_to_silver.ipynb
├── 04_Agregation_silver_to_gold.ipynb
```

## Tables

```
Example of sctrcuture from table name:
(Layer)_(schema)_(name)
```

![image](./assets/conventional_name_tables.jpg)



## Dags

```
Example of sctrcuture from Dag name:
(Layer)_(schema)_(name)
```