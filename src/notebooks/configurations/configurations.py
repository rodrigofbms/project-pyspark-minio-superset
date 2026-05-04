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