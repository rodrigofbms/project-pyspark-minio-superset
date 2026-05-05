# Path from landing, bronze, silver and Gold in MinIO S3
data_lakehouse_path ={
    "landing": "s3a://landing/adventureworks/",
    "bronze": "s3a://bronze/adventureworks/",
    "silver": "s3a://silver/adventureworks/",
    "gold": "s3a://gold/adventureworks/"
    
}


# Path from Bronze tables from postgres adventureworks
tables_postgres_adventureworks = {
    "1": "sales.countryregioncurrency",
    "2": "sales.creditcard",
    "3": "sales.currency",
    "4": "sales.currencyrate",
    "5": "sales.customer",
    "6": "sales.personcreditcard",
    "7": "sales.salesorderdetail",
    "8": "sales.salesorderheader",
    "9": "sales.salesorderheadersalesreason",
    "10": "sales.salesperson",
    "11": "sales.salespersonquotahistory",
    "12": "sales.salesreason",
    "13": "sales.salestaxrate",
    "14": "sales.salesterritory",
    "15": "sales.salesterritoryhistory",
    "16": "sales.shoppingcartitem",
    "17": "sales.specialoffer",
    "18": "sales.specialofferproduct",
    "19": "sales.store",
    "20": "humanresources.employee",
    "21": "humanresources.department"
}


queries_silver = {
    "sales_countryregioncurrency" :
    f""" 
    SELECT * 
    from delta.`{{bronze_path}}bronze_sales_countryregioncurrency` 
    """,
    
    "sales_creditcard" :
    f""" 
    SELECT * 
    from delta.`{{bronze_path}}bronze_sales_creditcard` 
    """,

    "sales_currency" :
    f""" 
    SELECT * 
    FROM delta.`{{bronze_path}}bronze_sales_currency` 
    """,

    "sales_currencyrate" :
    f"""
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_currencyrate` 
    """,

     "sales_customer" :
    f"""
    SELECT
        customerid,
		coalesce(personid, -1),
		coalesce(storeid, -1),
		territoryid,
		rowguid,
        month_key,
		modifieddate
    from delta.`{{bronze_path}}bronze_sales_customer` 
    """,

    "sales.personcreditcard":
    f""" 
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales.personcreditcard` 
    """,

    "sales_salesorderdetail" :
    f"""
    SELECT * 
    FROM delta.`{{bronze_path}}bronze_sales_salesorderdetail` 
    """,
    
    "sales_salesorderheader" : 
    f""" 
    SELECT 
    salesorderid,
    customerid,
    COALESCE(salespersonid, -1),
    territoryid,
    billtoaddressid,
    shiptoaddressid,
    shipmethodid,
    COALESCE(creditcardid, -1) as creditcardid,
    COALESCE(currencyrateid, -1) as currencyrateid,
    CAST(subtotal as DECIMAL (10,2)),
    CAST(taxamt as DECIMAL (10,2)),
    CAST(freight as DECIMAL(10,2)),
    CAST(totaldue as DECIMAL(10,2)),
    orderdate,
    shipdate,
    duedate,
    modifieddate,
    month_key,
    rowguid
    FROM delta.`{{bronze_path}}bronze_sales_salesorderheader` 
    """,

    "sales_salesorderheadersalesreason" :
    f"""
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_salesorderheadersalesreason`
    """,

    "sales_salesperson":
    f"""
    SELECT 
    businessentityid,
    coalesce(territoryid, -1) as territoryid,
    coalesce(salesquota, -1) as salesquota,
    bonus,
    cast(commissionpct as NUMERIC(10,3)),
    cast(salesytd as numeric(10,2)),
    cast(saleslastyear as numeric(10,2)),
    rowguid,
    month_key,
    modifieddate
    FROM delta.`{{bronze_path}}bronze_sales_salesperson`
    """,

    "sales_salespersonquotahistory":
    f"""
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_salespersonquotahistory`
    """,

    "sales_salesreason":
    f"""
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_salesreason` 
    """,

    "sales_salestaxrate":
    f""" 
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_salestaxrate`
    """,

    "sales_territory":
    f""" 
    SELECT 
    territoryid,
    "name",
    countryregioncode,
    "group",
    CAST(salesytd as NUMERIC(10,2)),
    CAST(saleslastyear as NUMERIC(10,2)),
    costytd,
    costlastyear,
    rowguid,
    month_key,
    modifieddate
    FROM delta.`{{bronze_path}}bronze_sales_territory` 
    """,

    "sales_salesterritoryhistory":
    f"""
    SELECT 
    businessentityid,
    territoryid,
    startdate,
    COALESCE(enddate,'2099-12-12') as enddate,
    rowguid,
    month_key,
    modifieddate
    FROM delta.`{{bronze_path}}bronze_sales_salesterritoryhistory`
    """,

    "sales_shoppingcartitem":
    f""" 
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_shoppingcartitem`
    """,

    "sales_specialoffer":
    f""" 
    SELECT 
    specialofferid,
    description,
    discountpct,
    "type",
    category,
    minqty,
    COALESCE(maxqty, 0) as maxqty,
    startdate,
    enddate,
    modifieddate,
    month_key,
    rowguid
    FROM delta.`{{bronze_path}}bronze_sales_specialoffer` 
    """,

    "sales_specialofferproduct":
    f""" 
    SELECT *
    FROM delta.`{{bronze_path}}bronze_sales_specialofferproduct` 
    """,

    "sales_store":
    f"""
    SELECT
    businessentityid,
    "name",
    salespersonid,
    rowguid,
    month_key,
    modifieddate
    FROM delta.`{{bronze_path}}bronze_sales_store` 
    """,

    "humanresources_employee":
    f"""
    SELECT 
    businessentityid,
    nationalidnumber,
    jobtitle,
    gender,
    hiredate,
    modifieddate,
    month_key,
    rowguid
    FROM delta.`{{bronze_path}}bronze_humanresources_employee`
    """,

    "humanresources_department":
    f""" 
    SELECT *
    FROM delta.`{{bronze_path}}bronze_humanresources_department`
    """
    
    
    
}