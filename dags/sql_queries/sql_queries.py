create_table_query = f"""CREATE TABLE IF NOT EXISTS tb_anp_fuel_sales
                    (
                        year_month      DATE, 
                        uf              TEXT,
                        product         TEXT,
                        unit            TEXT,
                        volume          DOUBLE PRECISION,
                        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )"""

insert_query = ("""
    INSERT INTO tb_anp_fuel_sales VALUES(%s,%s,%s,%s,%s)
""")