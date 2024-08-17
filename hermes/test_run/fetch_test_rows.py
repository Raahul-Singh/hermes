from pathlib import Path

import pandas as pd

from hermes.sqlcl import SQLCL

PATH = Path(__file__).parent

TEST_QUERY_1k = """
SELECT TOP 1000
    p.objID,
    p.run,
    p.rerun,
    p.camcol,
    p.field,
    s.z,
    CASE
        WHEN s.z < 1 THEN 'lt_1'
        WHEN s.z >= 1 AND s.z < 2 THEN '1_to_2'
        WHEN s.z >= 2 AND s.z < 3 THEN '2_to_3'
        WHEN s.z >= 3 AND s.z < 4 THEN '3_to_4'
        WHEN s.z >= 4 AND s.z < 5 THEN '4_to_5'
        WHEN s.z >= 5 AND s.z < 6 THEN '5_to_6'
        ELSE 'ge_6'
    END AS z_range
FROM
    PhotoObj AS p
JOIN
    SpecObj AS s ON p.objID = s.bestObjID
WHERE
    p.type = 3  -- type 3 corresponds to galaxies
    AND s.zWarning = 0  -- ensures reliable redshift measurements
    AND s.z BETWEEN 0.1 AND 6  -- example redshift range
"""

if __name__ == "__main__":
    sql_cl = SQLCL()
    df = sql_cl.query_database(TEST_QUERY_1k)
    sql_cl.logger.info(f"Query returned {df.shape[0]} rows")
    df.to_csv(PATH / "test_query_1k.csv")
    sql_cl.logger.info(f"Data saved to {PATH / 'data/test_query_1k.csv'}")
