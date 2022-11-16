### show spark warehouse directory
```python
spark.sql('set spark.sql.warehouse.dir').select('value').first()
```

### print several properties
```python
spark.sql('set').show()
```


### select from a table
```python
spark.sql('select count(*) from retail_db.orders').show()
```