% This is generated by ESQL's AbstractFunctionTestCase. Do not edit it. See ../README.md for how to regenerate it.

```esql
ROW i=1, a=["a", "b"], b=[2, 3] | STATS MIN(i) BY a, b | SORT a ASC, b ASC
```

| MIN(i):integer | a:keyword | b:integer |
| --- | --- | --- |
| 1 | a | 2 |
| 1 | a | 3 |
| 1 | b | 2 |
| 1 | b | 3 |
