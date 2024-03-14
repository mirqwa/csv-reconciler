# csv-reconciler
## Introduction

The script `csv_reconciler.py` contains the logic to handle reconciliation on 2 files: source and target. To install the libraries required by the script, we need to run `pip install -r requirements.txt`.

## Params
```
--source: The path to the first csv file input. This is a required argument.
--target: The path to the second csv file input. This is a required argument.
--output: The path to write the reconciliation report. This is a required argument.
--column: Specifies a column to be used in the comparison when checking field discrepancies. It can be specified multiple times to specify multiple columns. This is an optional argument. When not specified, the common columns in the source and target will be used, except the first column that uniquely identifies the records.
--fuzzy_merge_output: The path to the csv output of the fuzzy matching of the non-identical records. This is also optional and should be used carefully especially for many rows.
```

## Examples

Given the following inputs,

`source.csv`
```
ID,Name,Date,Amount
001,John Doe,2023-01-01,100.00
002,Jane Smith,2023-01-02,201.50
003,Robert Brown,2023-01-03,300.75
005,Emily Black,2023-01-05,300.90
```

`target.csv`
```
ID,Name,Date,Amount
001,John Doe,2023-01-01,100.00
002,Jane Smith,2023-01-04,200.50
004,Emily White,2023-01-05,400.90
```

### Example 1
Running the script as follows will genarate `reconciliation_report.csv` below
```
python csv_reconciler.py --source source.csv --target target.csv -o reconciliation.csv
```

`reconciliation_report.csv`
```
Record Identifier,Type,Field,Source Value,Target Value
002,Field Discrepancy,Amount,201.50,200.50
002,Field Discrepancy,Date,2023-01-02,2023-01-04
003,Missing in Target,,,
004,Missing in Source,,,
005,Missing in Target,,,
```

### Example 2
Running the script as follows will generate the reconciliation report below
```
python csv_reconciler.py --source source.csv --target target.csv -o reconciliation.csv --column Date
```

`reconciliation_report.csv`
```
RRecord Identifier,Type,Field,Source Value,Target Value
002,Field Discrepancy,Date,2023-01-02,2023-01-04
003,Missing in Target,,,
004,Missing in Source,,,
005,Missing in Target,,,
```

### Example 3
The following command specifies the out path for the fuzzy matching which enable the fuzzy match logic to run.
```
python csv_reconciler.py --source source.csv --target target.csv -o reconciliation.csv --fuzzy_merge_output fuzzy_output.csv
```

The reconciliation report output will be similar to the example 1 above. In addition to the reconciliation output, the fuzzy match output for non-identical records will also be generated.

```fuzzy_output.csv```
```
source_ID,source_Name,source_Date,source_Amount,target_ID,target_Name,target_Date,target_Amount
005,Emily Black,2023-01-05,300.90,004,Emily White,2023-01-05,400.90
```

## Conclusion
I have tested the script performs quite well even up to a couple of millions of rows, that is when not running the fuzzy match on the non-identical rows.
