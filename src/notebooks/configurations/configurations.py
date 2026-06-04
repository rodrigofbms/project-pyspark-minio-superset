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
    "21": "humanresources.department",
    "22": "person.countryregion",
    "23": "person.person",
    "24": "person.stateprovince",
    "25": "person.address",
    "26": "purchasing.shipmethod",
    "27": "production.product"
}


tables_pk = {
    "sales_countryregioncurrency": "countryregioncode",
    "sales_creditcard" : "creditcardid",
    "sales_currency" : "currencycode",
    "sales_currencyrate" : "currencyrateid",
    "sales_customer" : "customerid",
    "sales_personcreditcard": "businessentityid",
    "sales_salesorderdetail" : "salesorderdetailid",
    "sales_salesorderheader" : "salesorderid",
    "sales_salesorderheadersalesreason" : "salesorderid",
    "sales_salesperson": "businessentityid",
    "sales_salespersonquotahistory": "businessentityid",
    "sales_salesreason": "salesreasonid",
    "sales_salestaxrate": "salestaxrateid",
    "sales_salesterritory": "territoryid",
    "sales_salesterritoryhistory": "businessentityid",
    "sales_shoppingcartitem": "shoppingcartitemid",
    "sales_specialoffer": "specialofferid",
    "sales_specialofferproduct": "specialofferid",
    "sales_store": "businessentityid",
    "humanresources_employee": "businessentityid",
    "humanresources_department": "departmentid",
    "person_address": "addressid",
    "person_countryregion": "countryregioncode",
    "person_person": "businessentityid",
    "person_stateprovince": "stateprovinceid",
    "production_product": "productid",
    "purchasing_shipmethod": "shipmethodid"
}



# ---------------------------
# ---- Silver tables --------
# ---------------------------

queries_silver = {
    "sales_countryregioncurrency" :
    f""" 
    SELECT * 
    from delta.`{{layer_path}}bronze_sales_countryregioncurrency` 
    """,
    
    "sales_creditcard" :
    f""" 
    SELECT * 
    from delta.`{{layer_path}}bronze_sales_creditcard` 
    """,

    "sales_currency" :
    f""" 
    SELECT * 
    FROM delta.`{{layer_path}}bronze_sales_currency` 
    """,

    "sales_currencyrate" :
    f"""
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_currencyrate` 
    """,

     "sales_customer" :
    f"""
    SELECT
    customerid,
    COALESCE(personid, -1) as personid,
    COALESCE(storeid, -1) as storeid,
    territoryid,
    rowguid,
    month_key,
    modifieddate
    FROM delta.`{{layer_path}}bronze_sales_customer` 
    """,

    "sales_personcreditcard":
    f""" 
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_personcreditcard` 
    """,

    "sales_salesorderdetail" :
    f"""
    SELECT * 
    FROM delta.`{{layer_path}}bronze_sales_salesorderdetail` 
    """,
    
    "sales_salesorderheader" : 
    f""" 
    SELECT 
    salesorderid,
    customerid,
    COALESCE(salespersonid, -1) as salespersonid,
    territoryid,
    billtoaddressid,
    shiptoaddressid,
    shipmethodid,
    COALESCE(creditcardid, -1) as creditcardid,
    COALESCE(currencyrateid, -1) as currencyrateid,
    CAST(subtotal as DECIMAL(10,2)),
    CAST(taxamt as DECIMAL(10,2)),
    CAST(freight as DECIMAL(10,2)),
    CAST(totaldue as DECIMAL(10,2)),
    orderdate,
    shipdate,
    duedate,
    modifieddate,
    month_key,
    row_hash
    FROM delta.`{{layer_path}}bronze_sales_salesorderheader` 
    """,

    "sales_salesorderheadersalesreason" :
    f"""
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_salesorderheadersalesreason`
    """,

    "sales_salesperson":
    f"""
    SELECT 
    businessentityid,
    COALESCE(territoryid, -1) as territoryid,
    COALESCE(salesquota, -1) as salesquota,
    bonus,
    CAST(commissionpct as NUMERIC(10,3)),
    CAST(salesytd as NUMERIC(10,2)),
    CAST(saleslastyear as NUMERIC(10,2)),
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_sales_salesperson`
    """,

    "sales_salespersonquotahistory":
    f"""
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_salespersonquotahistory`
    """,

    "sales_salesreason":
    f"""
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_salesreason` 
    """,

    "sales_salestaxrate":
    f""" 
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_salestaxrate`
    """,

    "sales_salesterritory":
    f""" 
    SELECT 
    territoryid,
    name,
    countryregioncode,
    "group",
    CAST(salesytd as NUMERIC(10,2)),
    CAST(saleslastyear as NUMERIC(10,2)),
    costytd,
    costlastyear,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_sales_salesterritory` 
    """,

    "sales_salesterritoryhistory":
    f"""
    SELECT 
    businessentityid,
    territoryid,
    startdate,
    COALESCE(enddate,'2099-12-12') as enddate,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_sales_salesterritoryhistory`
    """,

    "sales_shoppingcartitem":
    f""" 
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_shoppingcartitem`
    """,

    "sales_specialoffer":
    f""" 
    SELECT 
    specialofferid,
    description,
    discountpct,
    type,
    category,
    minqty,
    COALESCE(maxqty, 0) as maxqty,
    startdate,
    enddate,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_sales_specialoffer` 
    """,

    "sales_specialofferproduct":
    f""" 
    SELECT *
    FROM delta.`{{layer_path}}bronze_sales_specialofferproduct` 
    """,

    "sales_store":
    f"""
    SELECT
    businessentityid,
    name,
    salespersonid,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_sales_store` 
    """,

    "humanresources_employee":
    f"""
    SELECT 
    businessentityid,
    nationalidnumber,
    jobtitle,
    gender,
    hiredate,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_humanresources_employee`
    """,

    "humanresources_department":
    f""" 
    SELECT *
    FROM delta.`{{layer_path}}bronze_humanresources_department`
    """,

    "person_countryregion":
    f""" 
    SELECT * 
    FROM delta.`{{layer_path}}bronze_person_countryregion`
    """,

    "person_person":
    f"""
    SELECT 
    businessentityid,
    persontype,
    firstname,
    lastname,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_person_person` 
    """,

    "person_stateprovince":
    f"""
    SELECT 
    stateprovinceid,
    stateprovincecode,
    countryregioncode,
    name,
    territoryid,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_person_stateprovince`
    """,

    "person_address":
    f""" 
    SELECT 
    addressid,
    stateprovinceid,
    addressline1,
    city,
    postalcode,
    rowguid,
    month_key,
    modifieddate,
    row_hash
    FROM delta.`{{layer_path}}bronze_person_address`
    """,


    "purchasing_shipmethod":
    f""" 
    SELECT *
    FROM delta.`{{layer_path}}bronze_purchasing_shipmethod`
    """,

    "production_product":
    f""" 
    SELECT
    productid,
    name,
    sellstartdate,
    coalesce(sellenddate,'2099-12-12') as sellenddate,
    month_key,
    modifieddate,
    row_hash
    from delta.`{{layer_path}}bronze_production_product`
    """

}


gold_pks = {
    
    "sales_by_country": "country_code",
    "sales_per_customer": "customer_id",
    "sales_per_employee": "employee_id",
    "sales_per_city_country": "city_sale",
    "quantity_sales_per_ship_method": "ship_id",
    "sales_per_card_type": "card_type",
    "quantity_sales_per_product": "product_id"
    
}


# ---------------------------
# ------ Gold tables --------
# ---------------------------
queries_gold = {

    "sales_by_country":
    f"""
    SELECT
    cr.countryregioncode AS country_code,
    UPPER(cr.name) AS country,
    COUNT(*) AS sales_quantity,
    ROUND(SUM(s.subtotal)) AS total_sales,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderheader` s
    INNER JOIN delta.`{{layer_path}}silver_sales_salesterritory` st ON s.territoryid = st.territoryid
    INNER JOIN delta.`{{layer_path}}silver_person_countryregion` cr ON cr.countryregioncode = st.countryregioncode
    GROUP BY cr.name, s.last_update, cr.countryregioncode
    """,
    
    "sales_per_customer":
    f""" 
    SELECT
    p.businessentityid AS customer_id,
    UPPER(CONCAT(p.firstname, ' ' , p.lastname)) AS customer_name,
    ROUND(SUM(s.subtotal)) AS total,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderheader` s
    INNER JOIN delta.`{{layer_path}}silver_sales_customer` c ON s.customerid = c.customerid
    INNER JOIN delta.`{{layer_path}}silver_person_person` p ON p.businessentityid = c.personid
    GROUP BY p.businessentityid, p.firstname, p.lastname, s.last_update
    """,

    "sales_per_employee":
    f""" 
    SELECT
    p.businessentityid AS employee_id,
    UPPER(CONCAT(p.firstname, ' ' , p.lastname)) AS employee_name,
    COUNT(*) AS quantity_sales,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderheader` s 
    INNER JOIN delta.`{{layer_path}}silver_sales_salesperson` sp ON sp.businessentityid = s.salespersonid
    INNER JOIN delta.`{{layer_path}}silver_humanresources_employee` e ON e.businessentityid = sp.businessentityid
    INNER JOIN delta.`{{layer_path}}silver_person_person` p ON p.businessentityid = e.businessentityid
    GROUP BY p.businessentityid, p.firstname, p.lastname, s.last_update
    """,

    "sales_per_city_country":
    f"""
    SELECT 
    UPPER(cr.name) AS country_sale,
    UPPER(a.city) AS city_sale,
    COUNT(*) AS quantity_sales,
    ROUND(SUM(s.subtotal)) AS total_sales,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderheader` s
    INNER JOIN delta.`{{layer_path}}silver_person_address` a ON a.addressid = s.shiptoaddressid
    INNER JOIN delta.`{{layer_path}}silver_person_stateprovince` sp ON sp.stateprovinceid = a.stateprovinceid
    INNER JOIN delta.`{{layer_path}}silver_person_countryregion` cr ON cr.countryregioncode = sp.countryregioncode
    GROUP BY cr.name, a.city, s.last_update
    ORDER BY cr.name, a.city
    """,

    "quantity_sales_per_ship_method":
    f""" 
    SELECT
    sm.shipmethodid as ship_id,
    UPPER(sm.name) AS ship_name,
    sm.shipbase AS ship_base,
    sm.shiprate AS ship_rate,
    COUNT(*) AS quantity,
    SUM(CASE
    	WHEN (s.duedate - s.orderdate) > INTERVAL '14 days' THEN 1
    	ELSE 0
    	END) AS delayed_deliveries,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderheader` s
    INNER JOIN delta.`{{layer_path}}silver_purchasing_shipmethod` sm on sm.shipmethodid = s.shipmethodid
    GROUP BY sm.shipmethodid, sm.name, sm.shipbase, sm.shiprate, s.last_update
    """,

    "sales_per_card_type":
    f"""
    SELECT
    UPPER(cc.cardtype) AS card_type,
    COUNT(*) AS sales_quantity,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderheader` s 
    INNER JOIN delta.`{{layer_path}}silver_sales_creditcard` cc ON cc.creditcardid = s.creditcardid
    GROUP BY cc.cardtype, s.last_update
    """,

    "quantity_sales_per_product":
    f""" 
    SELECT
    p.productid AS product_id,
    UPPER(p.name) AS product_name,
    SUM(s.orderqty) AS quantity_sales,
    COUNT(s.specialofferid != 1) AS quantity_sales_discount,
    s.last_update
    FROM delta.`{{layer_path}}silver_sales_salesorderdetail` s
    INNER JOIN delta.`{{layer_path}}silver_sales_specialofferproduct` sop ON sop.productid = s.productid
    INNER JOIN delta.`{{layer_path}}silver_production_product` p ON p.productid = sop.productid
    GROUP BY p.productid, p.name, s.last_update
    ORDER BY quantity_sales DESC
    """




    
}







