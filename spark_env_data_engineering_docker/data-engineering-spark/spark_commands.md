### show spark warehouse directory
```python
spark.sql('set spark.sql.warehouse.dir').select('value').first()
```

### print several properties
```python
spark.sql('set').show()
```
