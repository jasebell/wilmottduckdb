# DuckDB Article for Wilmott.com

Synthetic data generation code for the DuckDB article for Wilmott.com

The full news article can be read here: [https://wilmott.com/tick-data-quackery/](https://wilmott.com/tick-data-quackery/)

You'll need the Golang runtime in order generate the data.

```
go run datagenerator.go
```

This will generate five billion rows of tick data for you. Be warned it will be a large file size. 


